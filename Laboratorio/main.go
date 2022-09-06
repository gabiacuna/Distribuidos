package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	pb "github.com/Kendovvul/Ejemplo/Proto"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMessageServiceServer
}
func (s *server) Intercambio (ctx context.Context, msg *pb.Message) (*pb.Message, error){
	fmt.Println(msg.Body)
	
	if msg.Esc != ""{
		fmt.Println("El equipo " + msg.Esc+ " ha llegado a ayudar!")
	}
	
	// TODO probabilidad de contencion
	if rand.Intn(100) < 60{
		return &pb.Message{Body: "SI", Esc:msg.Esc}, nil
	}
 	return &pb.Message{Body: "NO",}, nil
}

func main() {
	LabName := "Laboratiorio Pripyat" //nombre del laboratorio
	qName := "Emergencias" //nombre de la cola
	hostQ := "localhost" //ip del servidor de RabbitMQ 172.17.0.1
	connQ, err := amqp.Dial("amqp://guest:guest@"+hostQ+":5672") //conexion con RabbitMQ
	
	if err != nil {log.Fatal(err)}
	defer connQ.Close()

	ch, err := connQ.Channel()
	if err != nil{log.Fatal(err)}
	defer ch.Close()
	fmt.Println("Estamos en "+LabName)

	for {
		if rand.Intn(100) < 80{
			fmt.Println("Ha ocurrido un estallido en " + LabName + "!")
			//Mensaje enviado a la cola de RabbitMQ (Llamado de emergencia)
			err = ch.Publish("", qName, false, false,
			amqp.Publishing{
				Headers: nil,
				ContentType: "text/plain",
				Body: []byte(LabName),  //Contenido del mensaje
			})
	
			if err != nil{
			log.Fatal(err)
			}
			break
			// fmt.Println(LabName)
		} else{
			fmt.Println("No hay estallido en " + LabName)
		}
		time.Sleep(5*time.Second)
	}

	listener, err := net.Listen("tcp", ":50051") //conexion sincrona
	if err != nil {
		panic("La conexion no se pudo crear" + err.Error())
	}

	serv := grpc.NewServer()
	for {
		pb.RegisterMessageServiceServer(serv, &server{})
		if err = serv.Serve(listener); err != nil {
			panic("El server no se pudo iniciar" + err.Error())
		}
	}
}
