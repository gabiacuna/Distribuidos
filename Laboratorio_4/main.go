package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"os/signal"
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
var port_central string

func (s *server) Intercambio(ctx context.Context, msg *pb.Message) (*pb.Message, error) {

	if msg.Esc != "" {
		fmt.Println("El equipo " + msg.Esc + " ha llegado a contener el estallido!")
		this_esc = msg.Esc
	}

	if msg.Body == "STOP" {
		serv.Stop()
		return &pb.Message{Body: "", Esc: this_esc}, nil
	}
	if msg.Body == "END" {
		fmt.Println("Cerrando Laboratorio")
		defer serv.Stop()
		defer os.Exit(3)
		return &pb.Message{Body: " ", Esc: ""}, nil
	}
	fmt.Println(msg.Body)

	// TODO probabilidad de contencion
	if rand.Intn(100) < 60 {
		resolved = true
		fmt.Println("Si!")
		fmt.Println("Devolviendo escuadron " + this_esc)
		fmt.Println("---------------------------------------------\n")
		return &pb.Message{Body: "SI", Esc: this_esc}, nil
	} else {
		fmt.Println("No :c")
		return &pb.Message{Body: "NO"}, nil
	}
}

func main() {
	LabName := "Laboratorio Pripyat"                               //nombre del laboratorio
	qName := "Emergencias"                                         //nombre de la cola
	hostQ := "dist085"                                             //ip del servidor de RabbitMQ 172.17.0.1
	connQ, err := amqp.Dial("amqp://test:test@" + hostQ + ":5672") //conexion con RabbitMQ
	rand.Seed(time.Now().UnixNano())
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

	listener_close, err_c := net.Listen("tcp", ":50058") //conexion sincrona
	if err_c != nil {
		panic("La conexion no se pudo crear" + err_c.Error())
	}
	serv = grpc.NewServer()
	pb.RegisterMessageServiceServer(serv, &server{})

	go func() {
		if err_c = serv.Serve(listener_close); err_c != nil {
			panic("El server no se pudo iniciar" + err_c.Error())
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		for sig := range c {
			fmt.Println("Cerrando Laboratorio ctrl-c")
			fmt.Println(sig)
			connQ.Close()
			serv.Stop()
			os.Exit(3)
		}
	}()

	for {
		resolved = false
		for {
			fmt.Println("Analizando estado del Laboratorio...")
			if rand.Intn(100) < 80 {
				fmt.Println("\n---------------------------------------------")
				fmt.Println("Ha ocurrido un estallido en " + LabName + "! Mandando SOS a Central")
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
				fmt.Println("\nNo hay estallido en " + LabName + "\n")
			}
			time.Sleep(5 * time.Second)
		}

		listener, err := net.Listen("tcp", ":50054") //conexion sincrona
		if err != nil {
			panic("La conexion no se pudo crear" + err.Error())
		}

		serv = grpc.NewServer()
		// fmt.Println("New sever")

		// for {
		pb.RegisterMessageServiceServer(serv, &server{})
		// fmt.Println("service info")
		// fmt.Println(serv.GetServiceInfo())
		if err = serv.Serve(listener); err != nil {
			panic("El server no se pudo iniciar" + err.Error())
		}
	}
}
