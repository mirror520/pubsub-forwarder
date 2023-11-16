package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/yaml.v3"

	"github.com/mirror520/pubsub-forwarder/model"
	"github.com/mirror520/pubsub-forwarder/router"
)

func main() {
	app := &cli.App{
		Name:  "pubsub-forwarder",
		Usage: "pubsub-forwarder seamlessly bridges and transforms messages between multiple pubsub systems, facilitating effortless communication and integration.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "path",
				Usage:   "Specifies the working directory",
				EnvVars: []string{"FORWARDER_PATH"},
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

	hook := &lumberjack.Logger{
		Filename:   path + "/log/proxy.log",
		MaxSize:    100,
		MaxAge:     60,
		MaxBackups: 0,
		LocalTime:  true,
		Compress:   false,
	}

	syncers := []zapcore.WriteSyncer{
		zapcore.AddSync(os.Stdout),
		zapcore.AddSync(hook),
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

	svc := router.NewService(cfg)
	defer svc.Close()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	sign := <-quit
	fmt.Println(sign.String())

	return nil
}
