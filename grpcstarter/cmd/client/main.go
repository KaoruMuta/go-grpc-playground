package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	hellopb "grpcstarter/pkg/grpc"
	"io"
	"log"
	"os"

	_ "google.golang.org/genproto/googleapis/rpc/errdetails" // これがないと、grpc starter のビルド時に `proto: not found` のエラーが発生
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var (
	scanner *bufio.Scanner
	client  hellopb.GreetingServiceClient
)

func main() {
	fmt.Println("start gRPC Client.")

	scanner = bufio.NewScanner(os.Stdin)

	address := "localhost:8080"
	conn, err := grpc.Dial(
		address,
		grpc.WithUnaryInterceptor(myUnaryClientInteceptor1),
		grpc.WithStreamInterceptor(myStreamClientInteceptor1),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatal("Connection failed.")
		return
	}
	defer conn.Close()

	client = hellopb.NewGreetingServiceClient(conn)

	for {
		fmt.Println("1: send Request")
		fmt.Println("2: HelloServerStream")
		fmt.Println("3: HelloClientStream")
		fmt.Println("4: HelloBiStream")
		fmt.Println("5: exit")
		fmt.Print("please enter >")

		scanner.Scan()
		in := scanner.Text()

		switch in {
		case "1":
			Hello()

		case "2":
			HelloServerStream()

		case "3":
			HelloClientStream()

		case "4":
			HelloBiStreams()

		case "5":
			fmt.Println("bye.")
			goto M
		}
	}
M:
}

func Hello() {
	fmt.Println("Please enter your name.")
	scanner.Scan()
	name := scanner.Text()

	req := &hellopb.HelloRequest{
		Name: name,
	}
	ctx := context.Background()
	md := metadata.New(map[string]string{"type": "unary", "from": "client"})
	// context に metadata を付与することで、サーバー側で metadata を受け取ることができる
	ctx = metadata.NewOutgoingContext(ctx, md)

	var header, trailer metadata.MD
	res, err := client.Hello(ctx, req, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		if stat, ok := status.FromError(err); ok {
			fmt.Printf("code: %s\n", stat.Code())
			fmt.Printf("message: %s\n", stat.Message())
			fmt.Printf("details: %s\n", stat.Details())
		} else {
			fmt.Println(err)
		}
	} else {
		fmt.Println(header)
		fmt.Println(trailer)
		fmt.Println(res.GetMessage())
	}
}

func HelloServerStream() {
	fmt.Println("Please enter your name.")
	scanner.Scan()
	name := scanner.Text()

	req := &hellopb.HelloRequest{
		Name: name,
	}
	stream, err := client.HelloServerStream(context.Background(), req)
	if err != nil {
		fmt.Println(err)
		return
	}

	for {
		// クライアントはストリームからレスポンスを受け取るために Recv を用いる
		res, err := stream.Recv()
		// ストリームの終わりを検知するために io.EOF を用いる
		if errors.Is(err, io.EOF) {
			fmt.Println("all the responses have already received.")
			break
		}

		if err != nil {
			fmt.Println(err)
		}
		fmt.Println(res)
	}
}

func HelloClientStream() {
	stream, err := client.HelloClientStream(context.Background())
	if err != nil {
		fmt.Println(err)
		return
	}

	sendCount := 5
	fmt.Printf("Please enter %d names.\n", sendCount)
	for i := 0; i < sendCount; i++ {
		scanner.Scan()
		name := scanner.Text()

		// ストリームを通じて送りたいリクエストだけ送信
		if err := stream.Send(&hellopb.HelloRequest{
			Name: name,
		}); err != nil {
			fmt.Println(err)
			return
		}
	}

	// 送り終わった後に、ストリームを閉じてレスポンスを受け取るために CloseAndRecv を用いる
	res, err := stream.CloseAndRecv()
	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(res.GetMessage())
	}
}

func HelloBiStreams() {
	ctx := context.Background()
	md := metadata.New(map[string]string{"type": "stream", "from": "client"})
	ctx = metadata.NewOutgoingContext(ctx, md)

	stream, err := client.HelloBiStreams(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}

	sendNum := 5
	fmt.Printf("Please enter %d names.\n", sendNum)

	var sendEnd, recvEnd bool
	sendCount := 0

	// 送信処理と受信処理の両方が終わるまではループする
	for !(sendEnd && recvEnd) {
		// 送信処理
		if !sendEnd {
			scanner.Scan()
			name := scanner.Text()

			sendCount++
			if err := stream.Send(&hellopb.HelloRequest{
				Name: name,
			}); err != nil {
				fmt.Println(err)
				sendEnd = true
			}

			// 送信回数に達したら、送信を終了
			if sendCount == sendNum {
				sendEnd = true
				if err := stream.CloseSend(); err != nil {
					fmt.Println(err)
				}
			}
		}

		// 受信処理
		var headerMD metadata.MD
		if !recvEnd {
			if headerMD == nil {
				headerMD, err = stream.Header()
				if err != nil {
					fmt.Println(err)
				} else {
					fmt.Println(headerMD)
				}
			}

			if res, err := stream.Recv(); err != nil {
				if !errors.Is(err, io.EOF) {
					fmt.Println(err)
				}
				// io.EOF を検知したら、受信を終了
				recvEnd = true
			} else {
				fmt.Println(res.GetMessage())
			}
		}
	}

	trailerMD := stream.Trailer()
	fmt.Println(trailerMD)
}
