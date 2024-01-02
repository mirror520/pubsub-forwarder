package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"

	"github.com/mirror520/pubsub-forwarder/model"
	"github.com/mirror520/pubsub-forwarder/transport/http"

	router "github.com/mirror520/pubsub-forwarder"
)

func main() {
	app := &cli.App{
		Name:  "pubsub-forwarder",
		Usage: "pubsub-forwarder effortlessly connects and transforms messages between different pubsub systems, acting as a seamless intermediary.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "path",
				Usage:   "Specifies the working directory",
				EnvVars: []string{"FORWARDER_PATH"},
			},
			&cli.IntFlag{
				Name:    "port",
				Usage:   "Specifies the HTTP service port",
				Value:   8080,
				EnvVars: []string{"FORWARDER_PORT"},
			},
		},
		Action: run,
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}

	time.Sleep(3000 * time.Millisecond)
}

func run(cli *cli.Context) error {
	path := cli.String("path")
	if path == "" {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return err
		}

		path = homeDir + "/.forwarder"
	}

	encoderCfg := zap.NewDevelopmentEncoderConfig()
	encoder := zapcore.NewConsoleEncoder(encoderCfg)

	syncers := []zapcore.WriteSyncer{
		zapcore.AddSync(os.Stdout),
	}

	core := zapcore.NewCore(
		encoder,
		zapcore.NewMultiWriteSyncer(syncers...),
		zapcore.DebugLevel,
	)

	log := zap.New(core)
	defer log.Sync()

	zap.ReplaceGlobals(log)

	f, err := os.Open(path + "/config.yaml")
	if err != nil {
		log.Fatal(err.Error())
	}
	defer f.Close()

	var cfg *model.Config
	if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
		log.Fatal(err.Error())
	}

	cfg.SetPath(path)

	svc := router.NewService(cfg)
	svc = router.LoggingMiddleware(log)(svc)
	defer svc.Close()

	r := gin.Default()
	apiV1 := r.Group("/v1")

	// POST /routers/:from/replay/:to
	{
		endpoint := router.ReplayEndpoint(svc)
		apiV1.POST("/routers/:from/replay/:to", http.ReplayHandler(endpoint))
	}

	// DELETE /tasks/:id
	{
		endpoint := router.CloseTaskEndpoint(svc)
		apiV1.DELETE("/tasks/:id", http.CloseTaskHandler(endpoint))
	}

	go r.Run(":" + strconv.Itoa(cli.Int("port")))

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sign := <-quit
	fmt.Println(sign.String())

	return nil
}
