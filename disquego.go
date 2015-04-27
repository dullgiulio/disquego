package main 

import (
    "log"
    
    "bufio"
    "bytes"
    "fmt"
	"io"
	"time"
    "net"
)

type Client struct{
    conn net.Conn
    buf  *bufio.Reader
}

func NewClient(conn net.Conn) *Client {
    return &Client{
        conn: conn,
        buf: bufio.NewReader(conn),
    }
}

func (c *Client) Write(data []byte) (written int, err error) {
    return c.conn.Write(data)
}

func (c *Client) jobid() (string, error) {
    jobid, err := c.buf.ReadString('\n')
    if err != nil {
        return "", err
    }
    fmt.Printf("%s\n", jobid)
    return jobid, nil
}

type Job struct {
	QueueName string
    Timeout   time.Duration
	Replicate int
	Delay     time.Duration
	Retry     time.Duration
	TTL       time.Duration
	Maxlen    int
	Async     bool
	data      *bytes.Buffer
    datalen   int
}

func NewJob(queueName string) *Job {
    return &Job{QueueName: queueName, data: bytes.NewBuffer(nil)}
}

func (j Job) Write(data []byte) (int, error) {
    n, err := j.data.Write(data)
    j.datalen += n
    return n, err
}

func (j Job) writeData(w *bufio.Writer) (written int, err error) {
    var n int
    b := j.data.Bytes()
    for i := 0; i < len(b); i++ {
        switch b[i] {
        case '\n':
            n, err = w.Write([]byte(`\n`))
        case '\r':
            n, err = w.Write([]byte(`\r`))
        default:
            err = w.WriteByte(b[i])
            n = 1
        }
        written += n
        if err != nil {
            return
        }
    }
    return
}

func (j *Job) WriteTo(w io.Writer) (written int, err error) {
	var n int
    n, err = fmt.Fprintf(w, "ADDJOB %s ", j.QueueName)
	written += n
	if err != nil {
		return
	}
    n, err = j.writeData(bufio.NewWriter(w))
    written += n
	if err != nil {
		return
	}
	n, err = fmt.Fprintf(w, " %d", j.Timeout/time.Millisecond)
	written += n
	if err != nil {
		return
	}
	if j.Replicate != 0 {
		n, err = fmt.Fprintf(w, " REPLICATE %d", j.Replicate)
		written += n
		if err != nil {
			return
		}
	}
	if j.Delay != 0 {
		n, err = fmt.Fprintf(w, " DELAY %d", j.Delay/time.Second)
		written += n
		if err != nil {
			return
		}
	}
	if j.Retry != 0 {
		n, err = fmt.Fprintf(w, " RETRY %d", j.Retry/time.Second)
		written += n
		if err != nil {
			return
		}
	}
	if j.TTL != 0 {
		n, err = fmt.Fprintf(w, " TTL %d", j.TTL/time.Second)
		written += n
		if err != nil {
			return
		}
	}
	if j.Maxlen != 0 {
		n, err = fmt.Fprintf(w, " MAXLEN %d", j.Maxlen)
		written += n
		if err != nil {
			return
		}
	}
	if j.Async {
		n, err = fmt.Fprintf(w, " ASYNC")
		written += n
	    if err != nil {
            return
        }
    }
	n, err = w.Write([]byte("\r\n"))
    written += n
    return
}

func getjob(w io.Writer, timeout time.Duration, count int, queues ...string) (written int, err error) {
	var n int
	n, err = fmt.Fprint(w, "GETJOB ")
	written += n
	if err != nil {
		return
	}
	if timeout != 0 {
		n, err = fmt.Fprintf(w, " TIMEOUT %d", timeout/time.Millisecond)
		written += n
		if err != nil {
			return
		}
	}
	if count != 0 {
		n, err = fmt.Fprintf(w, " COUNT %d", count)
		written += n
		if err != nil {
			return
		}
	}
	n, err = fmt.Fprint(w, " FROM ")
	written += n
	if err != nil {
		return
	}
    // TODO: Finish
	for _, queue := range queues {
		n, err = fmt.Fprintf(w, " %s", queue)
		written += n
		if err != nil {
			return
		}
	}
    n, err = w.Write([]byte("\r\n"))
    written += n
	return
}

func ack(w io.Writer, ack string, jobids ...string) (written int, err error) {
	var n int
	n, err = fmt.Fprint(w, ack)
	written += n
	if err != nil {
		return
	}
	for _, jobid := range jobids {
		n, err = fmt.Fprintf(w, " %s", jobid)
		written += n
		if err != nil {
			return
		}
	}
	n, err = w.Write([]byte("\r\n"))
    written += n
    return
}

func ackjob(w io.Writer, jobids ...string) (written int, err error) {
    return ack(w, "ACKJOB", jobids...)
}

func fastack(w io.Writer, jobids ...string) (written int, err error) {
    return ack(w, "FASTACK", jobids...)
}

func main() {
    conn, err := net.Dial("tcp", "localhost:7711")
    if err != nil {
        log.Fatal(err)
    }
    
    client := NewClient(conn)
    job := NewJob("test-queue")
    if _, err := fmt.Fprint(job, "somedata"); err != nil {
        fmt.Print(err)
    }
    
    if _, err := job.WriteTo(client); err != nil {
        fmt.Print(err)
    }
    client.jobid() 
}
