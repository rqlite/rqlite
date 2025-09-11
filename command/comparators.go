package command

import (
	"bytes"

	"github.com/rqlite/rqlite/v8/command/proto"
)

// CDCValueEqual returns true if two CDCValue objects are equal.
func CDCValueEqual(v1, v2 *proto.CDCValue) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}

	switch v1.GetValue().(type) {
	case *proto.CDCValue_I:
		v2i, ok := v2.GetValue().(*proto.CDCValue_I)
		if !ok {
			return false
		}
		return v1.GetI() == v2i.I
	case *proto.CDCValue_D:
		v2d, ok := v2.GetValue().(*proto.CDCValue_D)
		if !ok {
			return false
		}
		return v1.GetD() == v2d.D
	case *proto.CDCValue_S:
		v2s, ok := v2.GetValue().(*proto.CDCValue_S)
		if !ok {
			return false
		}
		return v1.GetS() == v2s.S
	case *proto.CDCValue_B:
		v2b, ok := v2.GetValue().(*proto.CDCValue_B)
		if !ok {
			return false
		}
		return v1.GetB() == v2b.B
	case *proto.CDCValue_Y:
		v2y, ok := v2.GetValue().(*proto.CDCValue_Y)
		if !ok {
			return false
		}
		return bytes.Equal(v1.GetY(), v2y.Y)
	default:
		return false
	}
}
