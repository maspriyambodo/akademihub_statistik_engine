package handler

import (
	"net/http"
	"strconv"
	"time"

	"github.com/sekolahpintar/statistik-engine/internal/service"
)

type StatistikHandler struct {
	svc *service.StatistikService
}

func NewStatistikHandler(svc *service.StatistikService) *StatistikHandler {
	return &StatistikHandler{svc: svc}
}

// ─── helpers ──────────────────────────────────────────────────────────────

func queryInt64(r *http.Request, key string) *int64 {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return nil
	}
	v, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return nil
	}
	return &v
}

func queryInt(r *http.Request, key string, fallback int) int {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return v
}

func queryFloat(r *http.Request, key string, fallback float64) float64 {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return v
}

// ─── handlers ─────────────────────────────────────────────────────────────

// GET /api/v1/statistik/overview
func (h *StatistikHandler) Overview(w http.ResponseWriter, r *http.Request) {
	result, err := h.svc.GetOverview(r.Context())
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Overview statistics retrieved successfully")
}

// GET /api/v1/statistik/akademik
// Query params: kelas_id, tahun_ajaran_id
func (h *StatistikHandler) Akademik(w http.ResponseWriter, r *http.Request) {
	kelasID := queryInt64(r, "kelas_id")
	tahunAjaranID := queryInt64(r, "tahun_ajaran_id")

	result, err := h.svc.GetAkademik(r.Context(), tahunAjaranID, kelasID)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Akademik statistics retrieved successfully")
}

// GET /api/v1/statistik/kehadiran
// Query params: kelas_id, tahun_ajaran_id, bulan, tahun
func (h *StatistikHandler) Kehadiran(w http.ResponseWriter, r *http.Request) {
	kelasID := queryInt64(r, "kelas_id")
	tahunAjaranID := queryInt64(r, "tahun_ajaran_id")
	bulan := queryInt(r, "bulan", 0)
	tahun := queryInt(r, "tahun", time.Now().Year())

	result, err := h.svc.GetKehadiran(r.Context(), tahunAjaranID, kelasID, bulan, tahun)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Kehadiran statistics retrieved successfully")
}

// GET /api/v1/statistik/keuangan
// Query params: tahun
func (h *StatistikHandler) Keuangan(w http.ResponseWriter, r *http.Request) {
	tahun := queryInt(r, "tahun", time.Now().Year())

	result, err := h.svc.GetKeuangan(r.Context(), tahun)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Keuangan statistics retrieved successfully")
}

// GET /api/v1/statistik/bk
// Query params: tahun, kelas_id
func (h *StatistikHandler) BK(w http.ResponseWriter, r *http.Request) {
	tahun := queryInt(r, "tahun", time.Now().Year())
	kelasID := queryInt64(r, "kelas_id")

	result, err := h.svc.GetBK(r.Context(), tahun, kelasID)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "BK statistics retrieved successfully")
}

// GET /api/v1/statistik/ppdb
// Query params: tahun
func (h *StatistikHandler) PPDB(w http.ResponseWriter, r *http.Request) {
	tahun := queryInt(r, "tahun", time.Now().Year())

	result, err := h.svc.GetPPDB(r.Context(), tahun)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "PPDB statistics retrieved successfully")
}

// GET /api/v1/statistik/perpustakaan
// Query params: tahun
func (h *StatistikHandler) Perpustakaan(w http.ResponseWriter, r *http.Request) {
	tahun := queryInt(r, "tahun", time.Now().Year())

	result, err := h.svc.GetPerpustakaan(r.Context(), tahun)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Perpustakaan statistics retrieved successfully")
}

// GET /api/v1/statistik/ujian
// Query params: kelas_id, mapel_id, semester, kkm
func (h *StatistikHandler) Ujian(w http.ResponseWriter, r *http.Request) {
	kelasID := queryInt64(r, "kelas_id")
	mapelID := queryInt64(r, "mapel_id")
	semester := r.URL.Query().Get("semester")
	kkm := queryFloat(r, "kkm", 75.0)

	result, err := h.svc.GetUjian(r.Context(), kelasID, mapelID, semester, kkm)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Ujian statistics retrieved successfully")
}

// GET /api/v1/statistik/ekstrakurikuler
// Query params: tahun
func (h *StatistikHandler) Ekstrakurikuler(w http.ResponseWriter, r *http.Request) {
	tahun := queryInt(r, "tahun", time.Now().Year())

	result, err := h.svc.GetEkstrakurikuler(r.Context(), tahun)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Ekstrakurikuler statistics retrieved successfully")
}

// GET /api/v1/statistik/organisasi
func (h *StatistikHandler) Organisasi(w http.ResponseWriter, r *http.Request) {
	result, err := h.svc.GetOrganisasi(r.Context())
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Organisasi statistics retrieved successfully")
}

// GET /api/v1/statistik/guru
// Query params: start_date (YYYY-MM-DD), end_date (YYYY-MM-DD)
func (h *StatistikHandler) Guru(w http.ResponseWriter, r *http.Request) {
	now := time.Now()
	defaultEnd := now.Format("2006-01-02")
	defaultStart := now.AddDate(0, -1, 0).Format("2006-01-02")

	startDate := r.URL.Query().Get("start_date")
	if startDate == "" {
		startDate = defaultStart
	}
	endDate := r.URL.Query().Get("end_date")
	if endDate == "" {
		endDate = defaultEnd
	}

	result, err := h.svc.GetGuru(r.Context(), startDate, endDate)
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "Guru statistics retrieved successfully")
}

// GET /api/v1/statistik/spk
func (h *StatistikHandler) SPK(w http.ResponseWriter, r *http.Request) {
	result, err := h.svc.GetSPK(r.Context())
	if err != nil {
		jsonServerError(w, err.Error())
		return
	}
	jsonOK(w, result, "SPK statistics retrieved successfully")
}
