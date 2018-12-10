package main

import (
    "bytes"
    "fmt"
    "time"
    "net/http"
    "io/ioutil"

    "gopkg.in/alexcesaro/statsd.v2"
    log "github.com/inconshreveable/log15"
    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/service/lambda"
)


func init() {
    /*
    Safety net for 'too many open files' issue on legacy code.
    Set a sane timeout duration for the http.DefaultClient, to ensure idle connections are terminated.
    Reference: https://stackoverflow.com/questions/37454236/net-http-server-too-many-open-files-error
    */
    http.DefaultClient.Timeout = time.Minute * 2
}

type Server struct {
    stats *statsd.Client
    invoker *lambda.Lambda
    callbackAddr string

    client *http.Client
}

func (s *Server) initServer() {
    s.client = &http.Client{ Timeout: time.Second * 5 }
}

func (s *Server) invoke(payload []byte, fn string) {
    // TODO time the time taken to send to callback as well?
    // TODO see if we can reuse a single http socket internally?
    url := fmt.Sprintf("http://%s/cb/%s", s.callbackAddr, fn)
    failurl := fmt.Sprintf("http://%s/cb/%s/error", s.callbackAddr, fn)

    timer := s.stats.NewTiming()
    result, err := s.invoker.Invoke(&lambda.InvokeInput{
        FunctionName: aws.String(fn),
        Payload: payload,
    })

    if err != nil {
        log.Error("Failed to invoke lambda", "error", err)
        timer.Send(fmt.Sprintf("invoke.%s.failure", fn))
        http.Post(failurl, "application/json", bytes.NewReader(payload))
        return
    }

    timer.Send(fmt.Sprintf("invoke.%s.success", fn))

    // NOTE avoiding json decode/encode by assuming payload is json and concatenating?
    buf := bytes.NewBufferString("{\"result\":")
    if _, err := buf.Write(result.Payload); err != nil {
        log.Error("Failed to construct json payload", "error", err)
        return
    }

    if _, err := buf.WriteString(", \"payload\":"); err != nil {
        log.Error("Failed to construct json payload", "error", err)
        return
    }

    if _, err := buf.Write(payload); err != nil {
        log.Error("Failed to construct json payload", "error", err)
        return
    }

    if _, err := buf.WriteString("}"); err != nil {
        log.Error("Failed to construct json payload", "error", err)
        return
    }

    resp, err := s.client.Post(url, "application/json", buf)
    defer resp.Body.Close()
    if err != nil {
        log.Error("failed to send response to callback", "error", err)
    }
    log.Info("Sent callback", "resp", resp)
}

func (s *Server) handleSingleURL(w http.ResponseWriter, req *http.Request) {
    payload, err := ioutil.ReadAll(req.Body)
    if err != nil {
        // TODO send err in callback
        log.Error("failed to read body", "error", err)
        return
    }

    w.WriteHeader(http.StatusAccepted)
    fmt.Fprintf(w, "Submitted");

    go s.invoke(payload, "handle_single_url")
}

func (s *Server) handleMultipleURLs(w http.ResponseWriter, req *http.Request) {
    payload, err := ioutil.ReadAll(req.Body)
    if err != nil {
        // TODO send err in callback
        log.Error("failed to read body", "error", err)
        return
    }

    w.WriteHeader(http.StatusAccepted)
    fmt.Fprintf(w, "Submitted");

    go s.invoke(payload, "handle_multiple_urls")
}

func (s *Server) handler(w http.ResponseWriter, r *http.Request) {
    s.stats.Count("my.count", 10)
    fmt.Fprintf(w, "Hi there, I love %s!", r.URL.Path[1:])
}
