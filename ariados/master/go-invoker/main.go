package main

import (
    "fmt"
    "flag"
    "net/http"
    "time"

    "gopkg.in/alexcesaro/statsd.v2"
    log "github.com/inconshreveable/log15"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/lambda"
)

type Args struct {
    Port int
    StatsdAddr string
    StatsdPrefix string
    CallbackAddr string
}

func prepareArgs(args *Args) {
    if args == nil {
        return
    }

    flag.IntVar(&args.Port, "port", 13001, "port for server")
    flag.StringVar(&args.StatsdAddr, "statsdAddr", "127.0.0.1:8125", "statsd addr")
    flag.StringVar(&args.StatsdPrefix, "statsdPrefix", "ariados.golang.", "statsd prefix")
    flag.StringVar(&args.CallbackAddr, "callbackAddr", "127.0.0.1:9999", "callback addr")
}

func main() {
    args := &Args{}
    prepareArgs(args)
    flag.Parse()
    log.Info("args are %+v", "args", args)

    stats, err := statsd.New(statsd.Address(args.StatsdAddr),
        statsd.FlushPeriod(500 * time.Millisecond), statsd.Prefix("ariados.golang"))

    if err != nil {
        log.Error("failed to connect to stats", "error", err)
    }
    defer stats.Close()

    sess := session.Must(session.NewSessionWithOptions(session.Options{
    SharedConfigState: session.SharedConfigEnable,
    }))

    // TODO ensure AWS_SDK_LOAD_CONFIG=1 AWS_PROFILE=ariados in env variables
    invoker := lambda.New(sess)

    server := &Server{
        stats: stats,
        invoker: invoker,
        callbackAddr: args.CallbackAddr,
    }

    server.initServer()

    http.HandleFunc("/single", server.handleSingleURL)
    http.HandleFunc("/multiple", server.handleMultipleURLs)
    if err := http.ListenAndServe(fmt.Sprintf(":%d", args.Port), nil); err != nil {
        log.Error("failed to run server", "err", err);
    }
}
