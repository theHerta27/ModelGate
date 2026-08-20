package api

import "github.com/gin-gonic/gin"

func NewRouter(handler *Handler) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", handler.Health)
	router.POST("/v1/chat/completions", handler.ChatCompletions)

	return router
}
