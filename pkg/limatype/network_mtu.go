// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package limatype

import (
	"bytes"
	"encoding/json"
	"fmt"

	"github.com/invopop/jsonschema"
)

// NetworkMTU is either a numeric L3 MTU or an explicit networks.yaml
// reference such as "cc.MTU". The scalar representation keeps maps, arrays,
// booleans, and fractional numbers out of a decoded Lima configuration.
type NetworkMTU struct {
	numeric   *uint32
	reference string
}

// NewNetworkMTU returns a numeric network MTU declaration.
func NewNetworkMTU(value uint32) *NetworkMTU {
	return &NetworkMTU{numeric: &value}
}

// NewNetworkMTUReference returns a networks.yaml MTU reference declaration.
func NewNetworkMTUReference(reference string) *NetworkMTU {
	return &NetworkMTU{reference: reference}
}

// Numeric returns the numeric declaration, if this value is numeric.
func (m NetworkMTU) Numeric() (uint32, bool) {
	if m.numeric == nil {
		return 0, false
	}
	return *m.numeric, true
}

// Reference returns the reference declaration, if this value is a reference.
func (m NetworkMTU) Reference() (string, bool) {
	if m.numeric != nil {
		return "", false
	}
	return m.reference, true
}

func (m *NetworkMTU) setNumeric(value uint64) error {
	if value > uint64(^uint32(0)) {
		return fmt.Errorf("network mtu integer %d exceeds uint32", value)
	}
	numeric := uint32(value)
	m.numeric = &numeric
	m.reference = ""
	return nil
}

func (m *NetworkMTU) setScalar(value any) error {
	switch value := value.(type) {
	case int:
		if value < 0 {
			return fmt.Errorf("network mtu integer %d must not be negative", value)
		}
		return m.setNumeric(uint64(value))
	case int64:
		if value < 0 {
			return fmt.Errorf("network mtu integer %d must not be negative", value)
		}
		return m.setNumeric(uint64(value))
	case uint32:
		return m.setNumeric(uint64(value))
	case uint64:
		return m.setNumeric(value)
	case string:
		m.numeric = nil
		m.reference = value
		return nil
	default:
		return fmt.Errorf("network mtu must be an integer or string, got %T", value)
	}
}

// UnmarshalYAML implements goccy/go-yaml's scalar unmarshaler interface.
func (m *NetworkMTU) UnmarshalYAML(unmarshal func(any) error) error {
	var value any
	if err := unmarshal(&value); err != nil {
		return err
	}
	return m.setScalar(value)
}

// MarshalYAML preserves the public scalar representation.
func (m NetworkMTU) MarshalYAML() (any, error) {
	if value, ok := m.Numeric(); ok {
		return value, nil
	}
	return m.reference, nil
}

// UnmarshalJSON accepts the same integer-or-string sum as YAML.
func (m *NetworkMTU) UnmarshalJSON(data []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	switch value := value.(type) {
	case json.Number:
		integer, err := value.Int64()
		if err != nil {
			return fmt.Errorf("network mtu must be an integer: %w", err)
		}
		return m.setScalar(integer)
	default:
		return m.setScalar(value)
	}
}

// MarshalJSON preserves the public scalar representation.
func (m NetworkMTU) MarshalJSON() ([]byte, error) {
	value, err := m.MarshalYAML()
	if err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

// JSONSchema advertises exactly the two scalar forms accepted by the decoder.
func (NetworkMTU) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{OneOf: []*jsonschema.Schema{
		{Type: "integer"},
		{Type: "string"},
	}}
}
