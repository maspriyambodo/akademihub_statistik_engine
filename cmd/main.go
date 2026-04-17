package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
	"github.com/joho/godotenv"
	"github.com/sekolahpintar/statistik-engine/internal/config"
	"github.com/sekolahpintar/statistik-engine/internal/db"
	"github.com/sekolahpintar/statistik-engine/internal/handler"
	"github.com/sekolahpintar/statistik-engine/internal/middleware"
	"github.com/sekolahpintar/statistik-engine/internal/repository"
	"github.com/sekolahpintar/statistik-engine/internal/service"
)

func main() {
	// Load .env (ignore error in production – env vars come from Docker)
	_ = godotenv.Load()

	cfg := config.Load()

	database, err := db.New(cfg)
	if err != nil {
		log.Fatalf("db: %v", err)
	}
	defer database.Close()

	// Dependency injection
	repo := repository.NewStatistikRepo(database)
	svc := service.NewStatistikService(repo)
	h := handler.NewStatistikHandler(svc)

	// Router
	r := chi.NewRouter()
	r.Use(chimw.RealIP)
	r.Use(chimw.Logger)
	r.Use(chimw.Recoverer)
	r.Use(chimw.Timeout(30 * time.Second))

	r.Get("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"statistik-engine"}`))
	})

	// All statistik routes require JWT auth
	r.Group(func(r chi.Router) {
		r.Use(middleware.Auth(cfg.JWTSecret, database))

		r.Route("/api/v1/statistik", func(r chi.Router) {
			r.Get("/overview", h.Overview)
			r.Get("/akademik", h.Akademik)
			r.Get("/kehadiran", h.Kehadiran)
			r.Get("/keuangan", h.Keuangan)
			r.Get("/bk", h.BK)
			r.Get("/ppdb", h.PPDB)
			r.Get("/perpustakaan", h.Perpustakaan)
			r.Get("/ujian", h.Ujian)
			r.Get("/ekstrakurikuler", h.Ekstrakurikuler)
			r.Get("/organisasi", h.Organisasi)
			r.Get("/guru", h.Guru)
			r.Get("/spk", h.SPK)
		})
	})

	srv := &http.Server{
		Addr:         ":" + cfg.AppPort,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("statistik-engine listening on :%s", cfg.AppPort)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-stop
	log.Println("shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown error: %v", err)
	}
	log.Println("stopped")
}
