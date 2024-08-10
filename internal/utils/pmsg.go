package utils

import (
	"fmt"
	"reflect"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// CustomProtoMessage wraps a regular proto message with custom marshaling and unmarshaling to make it
// json encoding and decoding safe.
//
// The regular JSON marshaller cannot operate correctly on proto messages
// as mentioned also in protojson golang specs.
type CustomProtoMessage[M protoreflect.ProtoMessage] struct {
	Msg M
}

func (pm *CustomProtoMessage[M]) MarshalJSON() ([]byte, error) {
	return protojson.Marshal(pm.Msg)
}

func (pm *CustomProtoMessage[M]) UnmarshalJSON(data []byte) error {
	m := pm.Msg.ProtoReflect().New().Interface()
	if err := protojson.Unmarshal(data, m); err != nil {
		return err
	}
	var typeok bool
	pm.Msg, typeok = m.(M)
	if !typeok {
		return fmt.Errorf("unexpected type found during unmarshal of msg")
	}
	return nil
}

func CreateCustomProtoMessage[M protoreflect.ProtoMessage](
	m M,
) *CustomProtoMessage[M] {
	if reflect.ValueOf(m).IsNil() {
		return nil
	}
	return &CustomProtoMessage[M]{Msg: m}
}

func CreateCustomProtoMessageIterableFrom[M protoreflect.ProtoMessage](
	source []M,
) []*CustomProtoMessage[M] {
	if source == nil {
		return nil
	}
	destination := make([]*CustomProtoMessage[M], 0, len(source))
	for _, msg := range source {
		destination = append(destination, CreateCustomProtoMessage(msg))
	}
	return destination
}
