package api

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/theHerta27/ModelGate/internal/service"
)

func RequestIDMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		requestID, err := service.NewRequestID()
		if err != nil {
			writeError(
				c,
				http.StatusServiceUnavailable,
				"service_unavailable_error",
				"request_id_unavailable",
				"request identifier generation is unavailable",
			)
			c.Abort()
			return
		}
		c.Header("X-Request-ID", requestID)
		c.Next()
	}
}
