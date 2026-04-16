package handler

import (
	"encoding/json"
	"net/http"
)

type apiResponse struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func jsonOK(w http.ResponseWriter, data interface{}, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(apiResponse{Success: true, Message: message, Data: data})
}

func jsonError(w http.ResponseWriter, code int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(apiResponse{Success: false, Message: message})
}

func jsonServerError(w http.ResponseWriter, msg string) {
	jsonError(w, http.StatusInternalServerError, msg)
}
