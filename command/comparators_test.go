package command

import (
	"testing"

	"github.com/rqlite/rqlite/v8/command/proto"
)

func Test_CDCValuesComparator(t *testing.T) {
	for i, tt := range []struct {
		v1    *proto.CDCValue
		v2    *proto.CDCValue
		equal bool
	}{
		{v1: nil, v2: nil, equal: true},
		{v1: nil, v2: &proto.CDCValue{}, equal: false},
		{v1: &proto.CDCValue{}, v2: nil, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_I{I: 1}}, v2: &proto.CDCValue{Value: &proto.CDCValue_I{I: 1}}, equal: true},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_I{I: 1}}, v2: &proto.CDCValue{Value: &proto.CDCValue_I{I: 2}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_I{I: 1}}, v2: &proto.CDCValue{Value: &proto.CDCValue_D{D: 1.0}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_D{D: 1.0}}, v2: &proto.CDCValue{Value: &proto.CDCValue_D{D: 1.0}}, equal: true},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_D{D: 1.0}}, v2: &proto.CDCValue{Value: &proto.CDCValue_D{D: 2.0}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_D{D: 1.0}}, v2: &proto.CDCValue{Value: &proto.CDCValue_I{I: 1}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_S{S: "foo"}}, v2: &proto.CDCValue{Value: &proto.CDCValue_S{S: "foo"}}, equal: true},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_S{S: "foo"}}, v2: &proto.CDCValue{Value: &proto.CDCValue_S{S: "bar"}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_S{S: "foo"}}, v2: &proto.CDCValue{Value: &proto.CDCValue_I{I: 1}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_B{B: true}}, v2: &proto.CDCValue{Value: &proto.CDCValue_B{B: true}}, equal: true},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_B{B: true}}, v2: &proto.CDCValue{Value: &proto.CDCValue_B{B: false}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_B{B: true}}, v2: &proto.CDCValue{Value: &proto.CDCValue_I{I: 1}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_Y{Y: []byte{0x01, 0x02}}}, v2: &proto.CDCValue{Value: &proto.CDCValue_Y{Y: []byte{0x01, 0x02}}}, equal: true},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_Y{Y: []byte{0x01, 0x02}}}, v2: &proto.CDCValue{Value: &proto.CDCValue_Y{Y: []byte{0x01, 0x03}}}, equal: false},
		{v1: &proto.CDCValue{Value: &proto.CDCValue_Y{Y: []byte{0x01, 0x02}}}, v2: &proto.CDCValue{Value: &proto.CDCValue_I{I: 1}}, equal: false},
	} {
		eq := CDCValueEqual(tt.v1, tt.v2)
		if eq != tt.equal {
			t.Fatalf("case %d: unexpected equality result: got %v, want %v", i, eq, tt.equal)
		}
	}
}
