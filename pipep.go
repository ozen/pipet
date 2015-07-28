package main

import (
    "fmt"
    "net"
    "time"
    "sync"
    "strings"
    "encoding/json"
    "os"
)

// Protocol corresponds to a unique integer for each supported application protocol
type Protocol string

const (
    // UNKNOWN is everything that is not supported
    UNKNOWN Protocol = "UNKNOWN"
    // AMQP protocol
    AMQP Protocol = "AMQP"
    // HTTP protocol
    HTTP Protocol = "HTTP"
    // SSH protocol
    SSH Protocol = "SSH"
)

// ConnectionInfo contains all variables that will be required
// to establish a connection in a single struct
type ConnectionInfo struct {
    Network, Address string
}

var frontends []ConnectionInfo
var backends map[Protocol]ConnectionInfo

func main() {
    frontends, backends = ReadConfig("config.json")

    if len(frontends) == 0 {
        fmt.Println("No frontends configured. Exiting.")
        return
    }

    var wg sync.WaitGroup

    for _, frontend := range frontends {
        wg.Add(1)

        go func(frontend ConnectionInfo) {
            defer wg.Done()

            ln, err := net.Listen(frontend.Network, frontend.Address)

            if err != nil {
              fmt.Printf("Error listening on %s: %s\n", frontend.Address, err.Error())
              return
            }

            defer ln.Close()

            fmt.Println("Listening on " + frontend.Address)

            for {
                conn, err := ln.Accept()
                if err != nil {
                    fmt.Println("Error accepting: ", err.Error())
                } else {
                    go handleConnection(conn)
                }
            }
        }(frontend)
    }

    wg.Wait()
}


func handleConnection(conn net.Conn) {
    defer conn.Close()
    buf := make([]byte, 16384)

    // read the first packet
    n, err := conn.Read(buf)
    if err != nil || n == 0 {
        fmt.Println("Error in the first message: ", err.Error())
        return
    }
    req := make([]byte, n)
    copy(req, buf[:n])

    // determine the application protocol
    protocol := GetApplicationProtocol(req)

    if protocol == UNKNOWN {
        fmt.Println("Unrecognized protocol.")
        return
    }

    if _, exists := backends[protocol]; !exists {
        fmt.Println("Protocol was recognized but does not have a backend.")
        return
    }

    // open connection to the correspoding backend
    backend := backends[protocol]
    backendConn, err := net.DialTimeout(backend.Network, backend.Address, time.Duration(30) * time.Second)
    if err != nil {
        fmt.Println("Error opening backend connection:", err.Error())
        return
    }
    defer backendConn.Close()

    backChan := ChannelFromConnection(backendConn, 5)
    frontChan := ChannelFromConnection(conn, 5)

    // send the first packet we received before. the backend still needs that.
    frontChan <- req

    PIPE: for {
        select {
        case b1 := <-backChan:
            if b1 == nil {
                break PIPE
            } else {
                conn.Write(b1)
            }
        case b2 := <-frontChan:
            if b2 == nil {
                break PIPE
            } else {
                backendConn.Write(b2)
            }
        }
    }
}

// GetApplicationProtocol determines the application protocol for the given
// packet. The packet must be the first packet sent by the client in the
// beginning of the connection.
func GetApplicationProtocol(packet []byte) Protocol {
    msg := string(packet)

    switch {
      // checking order is important
    case strings.Contains(msg, "HTTP"):
      return HTTP
    case strings.Contains(msg, "SSH"):
      return SSH
    case strings.Contains(msg, "AMQP"):
      return AMQP
    default:
      return UNKNOWN
    }
}

// ChannelFromConnection creates and returns a channel for the given connection
// and starts a goroutine that sends each packet received from the connection
// to the channel.
func ChannelFromConnection(conn net.Conn, size int) chan []byte {
  c := make(chan []byte, size)

  go func() {
    buf := make([]byte, 16384)

    for {
      n, err := conn.Read(buf)
      if n > 0 {
        res := make([]byte, n)
        copy(res, buf[:n])
        c <- res
      }
      if err != nil {
          c <- nil
          break
      }
    }
  }()

  return c
}

// ReadConfig reads JSON config file and returns frontends list and backends map
func ReadConfig(path string) ([]ConnectionInfo, map[Protocol]ConnectionInfo) {
    var config struct {
        Frontends []ConnectionInfo
        Backends map[Protocol]ConnectionInfo
    }

    configFile, err := os.Open(path)
    if err != nil {
        fmt.Println("Could not open config file: ", err.Error())
    }

    jsonParser := json.NewDecoder(configFile)
    if err = jsonParser.Decode(&config); err != nil {
        fmt.Println("Could not parse config file: ", err.Error())
    }

    return config.Frontends, config.Backends
}
