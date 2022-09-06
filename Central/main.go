package main

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	pb "github.com/Sistemas-Distribuidos-2022-2/Tarea1-Grupo22/Proto"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
)

func main() {
	qName := "Emergencias"                                           //Nombre de la cola
	hostQ := "localhost"                                             //Host de RabbitMQ 172.17.0.1
	hostS := "localhost"                                             //Host de un Laboratorio
	connQ, err := amqp.Dial("amqp://guest:guest@" + hostQ + ":5672") //Conexion con RabbitMQ

	if err != nil {
		log.Fatal(err)
	}
	defer connQ.Close()

	ch, err := connQ.Channel()
	if err != nil {
		log.Fatal(err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(qName, false, false, false, false, nil) //Se crea la cola en RabbitMQ
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(q)

	merc := 2

	fmt.Println("Esperando Emergencias")
	chDelivery, err := ch.Consume(qName, "", true, false, false, false, nil) //obtiene la cola de RabbitMQ
	if err != nil {
		log.Fatal(err)
	}

	for delivery := range chDelivery {
		fmt.Println("...")
		if merc > 0 {
			port := ":50051"                                           //puerto de la conexion con el laboratorio
			fmt.Println("Pedido de ayuda de " + string(delivery.Body)) //obtiene el primer mensaje de la cola
			merc--
			connS, err := grpc.Dial(hostS+port, grpc.WithInsecure()) //crea la conexion sincrona con el laboratorio

			if err != nil {
				panic("No se pudo conectar con el servidor" + err.Error())
			}

			// defer connS.Close()

			serviceCliente := pb.NewMessageServiceClient(connS)

			first_iter := true
			for {
				this_esc := ""
				if first_iter {
					this_esc = strconv.Itoa(merc) // para inf al lab sobre el escuadron mandado
					first_iter = false
				}
				//envia el mensaje al laboratorio
				res, err := serviceCliente.Intercambio(context.Background(),
					&pb.Message{
						Body: "Equipo listo?",
						Esc:  this_esc,
					})

				if err != nil {
					panic("No se puede crear el mensaje " + err.Error())
				}

				fmt.Println(res.Body) //respuesta del laboratorio
				if res.Body == "SI" {
					merc++

					// res, err := serviceCliente.Intercambio(context.Background(),
					// 	&pb.Message{
					// 		Body: "STOP",
					// 	})

					// if err != nil {
					// 	panic("No se puede crear el mensaje " + err.Error())
					// }

					fmt.Println("Ha vuelto a la central el equipo " + res.Esc)
					break
				}
				time.Sleep(5 * time.Second) //espera de 5 segundos

			}
			connS.Close()
		}
	}
}
