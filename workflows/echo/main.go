package main

import (
	"context"
	"fmt"
	"net/http"

	"github.com/dvaruas/restest/internal/generate/echo"
	"github.com/dvaruas/restest/internal/utils"
)

const serviceHost = "http://localhost:8081"

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	echoEndpoint := fmt.Sprintf("%v/api/service/echo", serviceHost)

	var req echo.EchoRequest = echo.EchoRequest{
		Msg: "Hello World",
	}
	var resp echo.EchoResponse

	_, err := utils.DoProtoHTTPCommunication(
		ctx,
		echoEndpoint,
		http.MethodPost,
		&req,
		nil,
		&resp)
	if err != nil {
		panic(err)
	}

	fmt.Printf("%v\n--> %v\n<-- %v\n", echoEndpoint, utils.PrettyPrintProto(&req), utils.PrettyPrintProto(&resp))
}
