package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"time"

	pb "github.com/Sistemas-Distribuidos-2022-2/Tarea1-Grupo22/Proto"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMessageServiceServer
}

var this_esc string
var resolved bool
var serv *grpc.Server

func (s *server) Intercambio(ctx context.Context, msg *pb.Message) (*pb.Message, error) {

	if msg.Esc != "" {
		fmt.Println("El equipo " + msg.Esc + " ha llegado a ayudar!")
		this_esc = msg.Esc
	}

	// if msg.Body == "STOP" {
	// 	serv.Stop()
	// 	return &pb.Message{}, nil
	// }
	fmt.Println(msg.Body)

	// TODO probabilidad de contencion
	if rand.Intn(100) < 60 {
		resolved = true

		fmt.Println("Devolviendo escuadron " + this_esc)

		return &pb.Message{Body: "SI", Esc: this_esc}, nil
	} else {
		return &pb.Message{Body: "NO"}, nil
	}
}

func main() {
	LabName := "Laboratiorio Pripyat"                                //nombre del laboratorio
	qName := "Emergencias"                                           //nombre de la cola
	hostQ := "localhost"                                             //ip del servidor de RabbitMQ 172.17.0.1
	connQ, err := amqp.Dial("amqp://guest:guest@" + hostQ + ":5672") //conexion con RabbitMQ

	if err != nil {
		log.Fatal(err)
	}
	defer connQ.Close()

	ch, err := connQ.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()
	fmt.Println("Estamos en " + LabName)

	for {
		resolved = false
		for {
			if rand.Intn(100) < 80 {
				fmt.Println("Ha ocurrido un estallido en " + LabName + "!")
				//Mensaje enviado a la cola de RabbitMQ (Llamado de emergencia)
				err = ch.Publish("", qName, false, false,
					amqp.Publishing{
						Headers:     nil,
						ContentType: "text/plain",
						Body:        []byte(LabName), //Contenido del mensaje
					})

				if err != nil {
					log.Fatal(err)
				}
				break
				// fmt.Println(LabName)
			} else {
				fmt.Println("No hay estallido en " + LabName)
			}
			time.Sleep(5 * time.Second)
		}

		listener, err := net.Listen("tcp", ":50051") //conexion sincrona
		if err != nil {
			panic("La conexion no se pudo crear" + err.Error())
		}

		serv = grpc.NewServer()
		fmt.Println("New sever")

		// for {
		pb.RegisterMessageServiceServer(serv, &server{})
		// fmt.Println("service info")
		// fmt.Println(serv.GetServiceInfo())
		if err = serv.Serve(listener); err != nil {
			panic("El server no se pudo iniciar" + err.Error())
		}
		// }
		fmt.Println("End of small for")

	}
}
