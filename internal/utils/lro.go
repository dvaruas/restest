package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

type LongrunningOperation[Req, Resp protoreflect.ProtoMessage] struct {
	OperationName  string
	OperationError error
	OperationDone  bool

	Request  Req
	Response Resp

	trigger func(ctx context.Context, req Req) (*longrunningpb.Operation, error)
	get     func(ctx context.Context, req *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error)
}

func CreateLongrunningOperation[Req, Resp protoreflect.ProtoMessage](
	trigger func(ctx context.Context, req Req) (*longrunningpb.Operation, error),
	get func(ctx context.Context, req *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error),
	req Req,
) *LongrunningOperation[Req, Resp] {
	return &LongrunningOperation[Req, Resp]{
		trigger: trigger,
		get:     get,
		Request: req,
	}
}

func (l *LongrunningOperation[Req, Resp]) Process(ctx context.Context) (bool, error) {
	if l.OperationError != nil {
		return true, l.OperationError
	}
	if l.OperationDone {
		return true, nil
	}

	var (
		opResult *longrunningpb.Operation
		err      error
	)

	if l.OperationName == "" {
		opResult, err = l.trigger(ctx, l.Request)
	} else {
		opResult, err = l.get(ctx, &longrunningpb.GetOperationRequest{
			Name: l.OperationName,
		})
	}
	if err != nil {
		l.OperationError = err
		return true, err
	}

	l.OperationName = opResult.GetName()
	l.OperationDone = opResult.GetDone()

	if !l.OperationDone {
		return false, nil
	}

	if opErr := opResult.GetError(); opErr != nil {
		l.OperationError = status.ErrorProto(opErr)
		return true, l.OperationError
	}

	respMsg, err := anypb.UnmarshalNew(opResult.GetResponse(), proto.UnmarshalOptions{DiscardUnknown: true})
	if err != nil {
		l.OperationError = fmt.Errorf("unmarshal resonse: %w", err)
		return true, l.OperationError
	}
	var typeok bool
	l.Response, typeok = respMsg.(Resp)
	if !typeok {
		l.OperationError = fmt.Errorf("unexpected response message of different type received: %v", respMsg)
		return true, l.OperationError
	}

	return true, nil
}

func (l *LongrunningOperation[Req, Resp]) MarshalJSON() ([]byte, error) {
	requestBytes, err := protojson.Marshal(l.Request)
	if err != nil {
		return nil, fmt.Errorf("protojson.Marshal of request: %w", err)
	}
	responseBytes, err := protojson.Marshal(l.Response)
	if err != nil {
		return nil, fmt.Errorf("protojson.Marshal of response: %w", err)
	}

	type Alias LongrunningOperation[Req, Resp]
	return json.Marshal(&struct {
		*Alias
		Requestbytes  []byte
		Responsebytes []byte
	}{
		Requestbytes:  requestBytes,
		Responsebytes: responseBytes,
		Alias:         (*Alias)(l),
	})
}

func (l *LongrunningOperation[Req, Resp]) UnmarshalJSON(data []byte) error {
	type Alias LongrunningOperation[Req, Resp]
	var uHolder struct {
		*Alias
		Requestbytes  []byte
		Responsebytes []byte
	}
	err := json.Unmarshal(data, &uHolder)
	if err != nil {
		return err
	}

	l.OperationDone = uHolder.OperationDone
	l.OperationError = uHolder.OperationError
	l.OperationName = uHolder.OperationName

	req := l.Request.ProtoReflect().New().Interface()
	if err := protojson.Unmarshal(uHolder.Requestbytes, req); err != nil {
		return err
	}
	l.Request = req.(Req)
	resp := l.Response.ProtoReflect().New().Interface()
	if err := protojson.Unmarshal(uHolder.Responsebytes, resp); err != nil {
		return err
	}
	l.Response = resp.(Resp)
	return nil
}

func (l *LongrunningOperation[Req, Resp]) GetRequest() Req {
	return l.Request
}

func (l *LongrunningOperation[Req, Resp]) GetResponse() Resp {
	return l.Response
}

func ProcessLongRunningOperationToCompletion[
	Req protoreflect.ProtoMessage,
	Resp protoreflect.ProtoMessage,
](
	ctx context.Context,
	l *LongrunningOperation[Req, Resp],
) error {
	var (
		innererr error
		isdone   bool
	)
	if err := RetryFunc(time.Minute*10, func() error {
		isdone, innererr = l.Process(ctx)
		if !isdone {
			return fmt.Errorf("trying continues")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("timed-out while retrying")
	}
	return innererr
}

func GetLongRunningOperationResult(
	ctx context.Context,
	name string,
	f func(context.Context, *longrunningpb.GetOperationRequest) (*longrunningpb.Operation, error),
	dst protoreflect.ProtoMessage,
) error {
	var (
		res *longrunningpb.Operation
		err error
	)

	if err := RetryFunc(time.Minute*10, func() error {
		res, err = f(ctx, &longrunningpb.GetOperationRequest{Name: name})
		if err != nil {
			return err
		}
		if res.Done {
			return nil
		}
		return fmt.Errorf("not done yet")
	}); err != nil {
		return err
	}

	if res.GetError() != nil {
		return fmt.Errorf("code: %v, message: %v", res.GetError().GetCode(), res.GetError().GetMessage())
	}

	err = anypb.UnmarshalTo(res.GetResponse(), dst, proto.UnmarshalOptions{DiscardUnknown: true})
	if err != nil {
		return err
	}
	return nil
}
