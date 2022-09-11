package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	pb "github.com/Sistemas-Distribuidos-2022-2/Tarea1-Grupo22/Proto"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedMessageServiceServer
}

var merc int

func custom_fatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func custom_panic(err error, msg string) {
	if err != nil {
		panic(msg + "\n error: " + err.Error())
	}
}

func custom_IDLE(conn *grpc.ClientConn, err error) {
	if conn.GetState().String() != "IDLE" {
		panic("No se puede cerrar la conexion " + err.Error())
	}
}

func log_consultas(file *os.File, lab_name string, n int) {
	line := lab_name + ";" + strconv.Itoa(n) + "\n"

	_, err := file.WriteString(line)

	custom_panic(err, "No se pudo escribir en el archivo")
}

func health_check(serviceCliente pb.MessageServiceClient, esc string) *pb.Message {
	res, err := serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "Estallido resuelto?",
			Esc:  esc,
		})
	custom_panic(err, "No se puede crear el mensaje")

	return res
}

func stop_lab_conn(serviceCliente pb.MessageServiceClient, conn *grpc.ClientConn) {
	_, err := serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "STOP",
		})
	custom_IDLE(conn, err)

}

func check_on_lab(serviceCliente pb.MessageServiceClient, port string, lab string, file *os.File, connS *grpc.ClientConn) {
	first_iter := true
	n_cons := 0
	for {
		this_esc := ""
		if first_iter {
			this_esc = strconv.Itoa(merc) // para inf al lab sobre el escuadron mandado
			merc--
			first_iter = false
		}
		//envia el mensaje al laboratorio
		res := health_check(serviceCliente, this_esc)

		n_cons++

		fmt.Println("Estallido resuelto?: " + res.Body) //respuesta del laboratorio
		// fmt.Println("Estallido resuelto?: " + res.Body) //respuesta del laboratorio

		if res.Body == "SI" {
			// fmt.Println("Ha vuelto a la central el equipo " + this_esc)
			fmt.Println("Ha vuelto a la central el equipo " + res.Esc + ", se ha cerrado la conexion con " + lab)

			fmt.Println("---------------------------------------------\n")
			merc++

			stop_lab_conn(serviceCliente, connS)

			log_consultas(file, lab, n_cons)

			// time.Sleep(5 * time.Second) // espera igual si el equipo resuelve el estallido
			return
		}
		time.Sleep(5 * time.Second) //espera de 5 segundos
	}
}

func send_merc(port string, hostS string, lab_name string, file *os.File) {
	fmt.Println("\n---------------------------------------------")               //puerto de la conexion con el laboratorio
	fmt.Println("Pedido de ayuda de " + lab_name + " [Mensaje asincrono leido]") //obtiene el primer mensaje de la cola
	fmt.Println("Enviando equipo " + strconv.Itoa(merc) + " a " + lab_name)

	conn, err := grpc.Dial(hostS+port, grpc.WithInsecure()) //crea la conexion sincrona con el laboratorio

	custom_panic(err, "No se pudo conectar con el servidor")

	defer conn.Close()

	serviceCliente := pb.NewMessageServiceClient(conn)

	check_on_lab(serviceCliente, port, lab_name, file, conn)

	fmt.Println("LLego al retuuuurn")
	return
}

func Shutdown(port [4]string, c chan os.Signal, hostS string, connMQ *amqp.Connection) {
	fmt.Println(c)
	flag := true
	for sig := range c {
		for _, p := range port {
			if flag {
				connMQ.Close()
				flag = false
			}
			connG, err := grpc.Dial(hostS+p, grpc.WithInsecure()) //crea la conexion sincrona con el laboratorio
			if err != nil {
				panic("No se pudo conectar con el servidor" + err.Error())
			}

			serviceCliente := pb.NewMessageServiceClient(connG)
			res1, err1 := serviceCliente.Intercambio(context.Background(),
				&pb.Message{
					Body: "END",
				})

			fmt.Println("Cerrando central y Laboratorios")
			if connG.GetState().String() != "IDLE" {
				panic("No se puede cerrar la conexion " + err1.Error())
			}
			// defer wg.Done()
			fmt.Println(sig)
			connG.Close()
			fmt.Println(res1)
		}
	}
	fmt.Println("Central cerrada")
	os.Exit(3)
}

func main() {
	qName := "Emergencias" //Nombre de la cola
	hostQ := "localhost"   //Host de RabbitMQ 172.17.0.1
	hostS := "dist085"     //Host de un Laboratorio

	port_lab1 := ":50051"
	port_lab2 := ":50052"
	port_lab3 := ":50053"
	port_lab4 := ":50054"

	// Puertos de cierre Laboratorios
	var close_port [4]string
	close_port[0] = ":50055"
	close_port[1] = ":50056"
	close_port[2] = ":50057"
	close_port[3] = ":50058"

	name_lab1 := "Laboratorio Renca"
	name_lab2 := "Laboratorio Pohang"
	name_lab3 := "Laboratorio Kampala"
	name_lab4 := "Laboratorio Pripyat"

	connQ, err := amqp.Dial("amqp://test:test@" + hostQ + ":5672") //Conexion con RabbitMQ
	custom_fatal(err)
	defer connQ.Close()

	ch, err := connQ.Channel()
	custom_fatal(err)
	defer ch.Close()

	q, err := ch.QueueDeclare(qName, false, false, false, false, nil) //Se crea la cola en RabbitMQ
	custom_fatal(err)

	fmt.Println(q)

	file, err := os.Create("SOLICITUDES.txt") //NombreLab;CantidadDeConsultas
	if err != nil {
		log.Fatal("No se pudo crear el archivo SOLICITUDES.txt: " + err.Error())
	}

	defer file.Close()

	merc = 2
	// var wg_shutdown sync.WaitGroup
	// wg_shutdown.Add(4)

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go Shutdown(close_port, c, hostS, connQ)
	// for _, p := range close_port {
	// fmt.Println("lab cerrado")
	// }

	// wg.Wait()
	fmt.Println("Esperando Emergencias")
	chDelivery, err := ch.Consume(qName, "", false, false, false, false, nil) //obtiene la cola de RabbitMQ
	custom_fatal(err)

	for delivery := range chDelivery {
		if merc > 0 {
			port_sos := ""
			delivery.Ack(false) // only ACK this msg

			lab := string(delivery.Body)

			if lab == name_lab1 {
				port_sos = port_lab1
			} else if lab == name_lab2 {
				port_sos = port_lab2
			} else if lab == name_lab3 {
				port_sos = port_lab3
			} else if lab == name_lab4 {
				port_sos = port_lab4
			}

			fmt.Println("\n------\n" + port_sos + "\n------\n" + lab)
			go send_merc(port_sos, hostS, lab, file)
		}
		time.Sleep(5 * time.Second) //espera de 5 segundos

	}
}
