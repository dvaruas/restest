package utils

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// DoProtoHTTPCommunication does HTTP communication over proto defined messages.
//
// Parameters:
// - url: URL to send request to.
// - httpMethod: HTTP method used to send request.
// - req: Proto message to be used as request body. (if nil, then no body is sent)
// - reqHeaders: Headers to set with HTTP request.
// - resp: on success, the response is decoded and set to this. (if nil, then no response body is expected)
//
// Returns:
// - statusCode: HTTP status code.
// - err: error.
func DoProtoHTTPCommunication(
	ctx context.Context,
	url, httpMethod string,
	req protoreflect.ProtoMessage,
	reqHeaders map[string]string,
	resp protoreflect.ProtoMessage,
) (statusCode int, err error) {
	var reqBody []byte
	if req != nil {
		reqBody, err = protojson.Marshal(req)
		if err != nil {
			return 0, err
		}
		reqHeaders = CaseInsensitiveKeyMapJoin(reqHeaders, map[string]string{
			"Content-type": "application/json",
		})
	}

	statusCode, respBody, err := DoHTTPCommunication(
		ctx,
		url,
		httpMethod,
		bytes.NewReader(reqBody),
		reqHeaders)
	if err != nil {
		return 0, err
	}

	if resp != nil {
		err = protojson.UnmarshalOptions{DiscardUnknown: true}.Unmarshal(respBody, resp)
		if err != nil {
			return statusCode, err
		}
	}

	return statusCode, nil
}

// DoHTTPCommunication is a thin-wrapper over basic HTTP communication.
//
// Parameters:
// - url: url to send request to.
// - httpMethod: HTTP method used to send request.
// - reqBody: message body to send with HTTP request. (if nil, then no body is sent)
// - reqHeaders: Headers to set with HTTP request.
//
// Returns:
// - statusCode: HTTP status code.
// - respBody: Response body.
// - err: error.
func DoHTTPCommunication(
	ctx context.Context,
	url, httpMethod string,
	reqBody io.Reader,
	reqHeaders map[string]string,
) (statusCode int, respBody []byte, err error) {
	r, err := http.NewRequestWithContext(ctx, httpMethod, url, reqBody)
	if err != nil {
		return 0, nil, err
	}

	for k, v := range reqHeaders {
		r.Header.Add(k, v)
	}

	statusCode, streamedRespBody, err := DoHTTPStreamedCommunication(
		ctx,
		url,
		httpMethod,
		reqBody,
		reqHeaders)
	if err != nil {
		return statusCode, nil, err
	}

	defer streamedRespBody.Close()

	b, err := io.ReadAll(streamedRespBody)
	if err != nil {
		return statusCode, nil, err
	}
	return statusCode, b, nil
}

// DoHTTPStreamedCommunication is a thin-wrapper over basic HTTP communication where the response is not read
// by the function but rather the caller is responsible the body contents and closing the reader when done.
//
// Parameters:
// - url: url to send request to.
// - httpMethod: HTTP method used to send request.
// - reqBody: message body to send with HTTP request. (if nil, then no body is sent)
// - reqHeaders: Headers to set with HTTP request.
//
// Returns:
// - statusCode: HTTP status code.
// - respBody: Response body reader. (only set when there is no error)
// - err: error.
func DoHTTPStreamedCommunication(
	ctx context.Context,
	url, httpMethod string,
	reqBody io.Reader,
	reqHeaders map[string]string,
) (statusCode int, respBody io.ReadCloser, err error) {
	r, err := http.NewRequestWithContext(
		ctx,
		httpMethod,
		url,
		reqBody)
	if err != nil {
		return 0, nil, err
	}

	for k, v := range reqHeaders {
		r.Header.Add(k, v)
	}

	rsp, err := http.DefaultClient.Do(r)
	if err != nil {
		return 0, nil, err
	}

	// Success response status codes will lie between 200-299 and below 200 is informational we ignore those as well.
	if rsp.StatusCode >= 300 {
		defer rsp.Body.Close()

		b, err := io.ReadAll(rsp.Body)
		if err != nil {
			return rsp.StatusCode, nil, err
		}
		return rsp.StatusCode, nil, fmt.Errorf("resp body : %v", string(b))
	}

	return rsp.StatusCode, rsp.Body, nil
}
