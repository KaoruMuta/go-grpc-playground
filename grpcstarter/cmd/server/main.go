package main

import (
	"context"
	"errors"
	"fmt"
	hellopb "grpcstarter/pkg/grpc"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

// myServer gRPC サーバーの実装
type myServer struct {
	hellopb.UnimplementedGreetingServiceServer
}

func NewMyServer() *myServer {
	return &myServer{}
}

func main() {
	port := 8080
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			myUnaryServerInterceptor1,
			myUnaryServerInterceptor2,
		),
		grpc.StreamInterceptor(myStreamServerInterceptor1),
	)

	hellopb.RegisterGreetingServiceServer(s, NewMyServer())

	// Failed to list services: server does not support the reflection API
	// のエラーが発生するため、サーバーリフレクションを追加
	// See: https://github.com/grpc/grpc/blob/master/doc/server-reflection.md
	reflection.Register(s)

	go func() {
		log.Printf("start gRPC server port: %v", port)
		s.Serve(listener)
	}()

	// Ctrl+C で graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	log.Println("stopping gRPC server")
	s.GracefulStop()
}

func (s *myServer) Hello(ctx context.Context, req *hellopb.HelloRequest) (*hellopb.HelloResponse, error) {
	// metadata を context から取得する
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		log.Println(md)
	}

	// ヘッダーの設定
	// SetHeaderメソッドは、ヘッダーが送られる前ならば何度でも呼び出すことができる
	// ヘッダーは、以下のうちいずれかがはじめに起こったときに送信される
	//   - SendHeaderが明示的に呼ばれるとき
	//   - 最初のメッセージ(レスポンス)が送信されるとき
	//   - ステータスコードがクライアントに返却されるとき
	headerMD := metadata.New(map[string]string{"type": "unary", "from": "server", "in": "header"})
	if err := grpc.SetHeader(ctx, headerMD); err != nil {
		return nil, err
	}

	// トレーラーの設定
	// トレーラーは、ステータスコードがクライアントに返却されるときに送信される
	// SetHeaderメソッドやSetTrailerメソッドを複数回呼ぶことで登録されたヘッダの情報は、(mapの更新のように)マージされて保持される
	trailerMD := metadata.New(map[string]string{"type": "unary", "from": "server", "in": "trailer"})
	if err := grpc.SetTrailer(ctx, trailerMD); err != nil {
		return nil, err
	}

	return &hellopb.HelloResponse{
		Message: fmt.Sprintf("Hello, %s!", req.GetName()),
	}, nil
}

func (s *myServer) HelloServerStream(req *hellopb.HelloRequest, stream hellopb.GreetingService_HelloServerStreamServer) error {
	resCount := 5
	for i := 0; i < resCount; i++ {
		// クライアントにレスポンスを返し続けるために Send を用いる
		if err := stream.Send(&hellopb.HelloResponse{
			Message: fmt.Sprintf("[%d] Hello, %s!", i, req.GetName()),
		}); err != nil {
			return err
		}
		time.Sleep(time.Second * 1)
	}

	// ストリームの終わり
	return nil
}

func (s *myServer) HelloClientStream(stream hellopb.GreetingService_HelloClientStreamServer) error {
	nameList := make([]string, 0)
	for {
		// クライアントからのリクエストを受け取るために Recv を用いる
		req, err := stream.Recv()
		// ストリームの終わりを検知するために io.EOF を用いる
		if errors.Is(err, io.EOF) {
			// メッセージを受け取り終わったあとの処理
			message := fmt.Sprintf("Hello, %v!", nameList)
			return stream.SendAndClose(&hellopb.HelloResponse{
				Message: message,
			})
		}
		if err != nil {
			return err
		}
		nameList = append(nameList, req.GetName())
	}
}

func (s *myServer) HelloBiStreams(stream hellopb.GreetingService_HelloBiStreamsServer) error {
	// metadata をストリームの context から取得する
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		log.Println(md)
	}

	// (パターン1)すぐにヘッダーを送信したいならばこちら
	headerMD := metadata.New(map[string]string{"type": "stream", "from": "server", "in": "header"})
	if err := stream.SendHeader(headerMD); err != nil {
		return err
	}
	// (パターン2)本来ヘッダーを送るタイミングで送りたいならばこちら
	if err := stream.SetHeader(headerMD); err != nil {
		return err
	}

	trailerMD := metadata.New(map[string]string{"type": "stream", "from": "server", "in": "trailer"})
	stream.SetTrailer(trailerMD)

	for {
		// クライアントからのリクエストを受け取るために Recv を用いる
		req, err := stream.Recv()
		// ストリームの終わりを検知するために io.EOF を用いる
		if errors.Is(err, io.EOF) {
			// メッセージを受け取り終わったあとの処理
			return nil
		}
		if err != nil {
			return err
		}
		message := fmt.Sprintf("Hello, %v!", req.GetName())
		// クライアントにレスポンスを返し続けるために
		// io.EOF を検知したら Send ではなく、都度 Send を実行
		if err := stream.Send(&hellopb.HelloResponse{
			Message: message,
		}); err != nil {
			return err
		}
	}
}
