# SkyNyet Lima: shared-memory networking for VZ guests

[Русская версия](README.ru.md)

This proof of concept (PoC) is a pair of forks based on [Lima v2.2.0](https://github.com/lima-vm/lima/blob/v2.2.0/README.md) and [socket_vmnet v1.2.2](https://github.com/lima-vm/socket_vmnet/blob/v1.2.2/README.md). They are not official releases.

## Why

The main motivation is to speed up vanilla shared networking. When two VMs exchange traffic, relaying every Ethernet frame through a stream socket adds process switches, copies, and syscalls to traffic that does not need vmnet at all.

The first version used a separate shared-memory bus coordinator; it now lives *inside* `socket_vmnet`. Lima automatically connects each VZ guest's hostagent to the bus through an additional Unix control socket. VM-to-VM frames travel directly through shared-memory rings, while the daemon serves as the vmnet uplink for host and external connectivity. For a Lima network named `shared`, the bus socket is `socket_vmnet_shm.shared`, beside the original `socket_vmnet.shared`.

## Features and fixes

- **Automatic bus attachment and fallback.** Lima finds the bus socket for VZ guests without a special flag. If it is absent, Lima warns and uses the existing framed path; if it is present but the connection fails, Lima reports an error.
- **One stalled VM does not block the others.** Each hostagent reads its own ring independently. A recipient holding a full slot may cause a frame to be dropped, but other participants do not wait for its socket.
- **Recovery without restarting the VM.** After a coordinator restart, the hostagent rejoins the bus. A stalled join can be cancelled when the VM stops.
- **Independent MTUs.** Lima sets the `socket_vmnet` segment MTU and a VZ network attachment's MTU separately. A VM's MTU may be numeric or refer to the network MTU; it is deliberately allowed to differ from the bus MTU, for example when testing PMTU discovery.
- **Batched VZ edge.** Darwin `recvmsg_x`/`sendmsg_x` process frames in batches in both directions; ordinary datagram operations remain available when those calls are not.

## Performance

- At MTU 1500, vanilla Lima with upstream `socket_vmnet` delivered 2.38 Gbps between two VMs; the shared-memory bus reached 9.88 Gbps (23.4 Gbps at MTU 9000).
- With the vmnet uplink present, two VMs sustained about 5.67 Gbps **in each direction (bidir)** at MTU 1500, with four TCP streams per direction.
- Stopping and rejoining an idle third VM left a concurrent host-to-VM transfer at 7.91, 7.84, and 7.72 Gbps. The two guests also kept exchanging traffic after the vmnet uplink was retired.
- VZ-edge batching cut hostagent CPU per GB by 30.2%.

The bus fast path serves VZ guests on `socket_vmnet` networks. QEMU, `user-v2`, and VZ NAT do not use it.

## Fork authorship

Igor Podlesny with AI agents (OpenAI GPT/Codex, Anthropic Claude, xAI Grok, Qwen via OpenCode, and others).

Original Lima and socket_vmnet authorship and licenses are preserved.
