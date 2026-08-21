package api

import (
	"log/slog"

	"github.com/gin-gonic/gin"

	"github.com/theHerta27/ModelGate/internal/metrics"
)

type RouterOptions struct {
	Metrics *metrics.Metrics
	Logger  *slog.Logger
}

func NewRouter(handler *Handler, options ...RouterOptions) *gin.Engine {
	router := gin.New()
	router.Use(RequestIDMiddleware())
	if len(options) > 0 && options[0].Metrics != nil {
		router.Use(options[0].Metrics.HTTPMiddleware(options[0].Logger))
	}
	router.Use(gin.Recovery())

	router.GET("/health", handler.Health)
	router.POST("/v1/chat/completions", handler.ChatCompletions)
	if len(options) > 0 && options[0].Metrics != nil {
		router.GET("/metrics", gin.WrapH(options[0].Metrics.Handler()))
	}

	return router
}
