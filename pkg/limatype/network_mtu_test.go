// SPDX-FileCopyrightText: Copyright The Lima Authors
// SPDX-License-Identifier: Apache-2.0

package limatype

import (
	"encoding/json"
	"testing"

	"gotest.tools/v3/assert"
)

func TestNetworkMTUJSONScalars(t *testing.T) {
	for _, tc := range []struct {
		input string
		want  string
	}{
		{`9000`, `9000`},
		{`"cc.MTU"`, `"cc.MTU"`},
	} {
		var mtu NetworkMTU
		assert.NilError(t, json.Unmarshal([]byte(tc.input), &mtu))
		encoded, err := json.Marshal(mtu)
		assert.NilError(t, err)
		assert.Equal(t, string(encoded), tc.want)
	}
	for _, input := range []string{`{}`, `[]`, `true`, `1.5`, `-1`, `4294967296`} {
		var mtu NetworkMTU
		assert.Assert(t, json.Unmarshal([]byte(input), &mtu) != nil, "unexpectedly accepted %s", input)
	}
}

func TestNetworkMTUSchemaIsIntegerOrString(t *testing.T) {
	schema := (NetworkMTU{}).JSONSchema()
	assert.Equal(t, len(schema.OneOf), 2)
	assert.Equal(t, schema.OneOf[0].Type, "integer")
	assert.Equal(t, schema.OneOf[1].Type, "string")
}
