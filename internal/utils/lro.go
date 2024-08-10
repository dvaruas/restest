package utils

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
)

type LongrunningOperation[Req, Resp protoreflect.ProtoMessage] struct {
	OperationName  string
	OperationError error
	OperationDone  bool

	Request  *CustomProtoMessage[Req]
	Response *CustomProtoMessage[Resp]

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
		Request: CreateCustomProtoMessage(req),
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
		opResult, err = l.trigger(ctx, l.Request.Msg)
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
	response, typeok := respMsg.(Resp)
	if !typeok {
		l.OperationError = fmt.Errorf("unexpected response message of different type received: %v", respMsg)
		return true, l.OperationError
	}
	l.Response = CreateCustomProtoMessage(response)

	return true, nil
}

func (l *LongrunningOperation[Req, Resp]) GetRequest() Req {
	var request Req
	if l.Request == nil {
		return request
	}
	return l.Request.Msg
}

func (l *LongrunningOperation[Req, Resp]) GetResponse() Resp {
	var response Resp
	if !l.OperationDone || l.OperationError != nil || l.Response == nil {
		return response
	}
	return l.Response.Msg
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
