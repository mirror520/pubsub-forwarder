package http

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-kit/kit/endpoint"

	router "github.com/mirror520/pubsub-forwarder"
)

func ReplayHandler(endpoint endpoint.Endpoint) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		request := router.ReplayRequest{
			From:  ctx.Param("from"),
			To:    ctx.Param("to"),
			Topic: ctx.Query("topic"),
		}

		if sinceStr := ctx.Query("since"); sinceStr != "" {
			since, err := time.ParseInLocation(time.RFC3339, sinceStr, time.Local)
			if err != nil {
				ctx.Abort()
				ctx.String(http.StatusBadRequest, err.Error())
				return
			}

			request.Since = since
		}

		id, err := endpoint(ctx, request)
		if err != nil {
			ctx.Abort()
			ctx.String(http.StatusUnprocessableEntity, err.Error())
			return
		}

		ctx.String(http.StatusOK, "%s", id)
	}
}

func CloseTaskHandler(endpoint endpoint.Endpoint) gin.HandlerFunc {
	return func(ctx *gin.Context) {
		id := ctx.Param("id")

		_, err := endpoint(ctx, id)
		if err != nil {
			ctx.Abort()
			ctx.String(http.StatusUnprocessableEntity, err.Error())
			return
		}

		ctx.String(http.StatusOK, "ok")
	}
}
