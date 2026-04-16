package service

import (
	"context"

	"github.com/sekolahpintar/statistik-engine/internal/model"
	"github.com/sekolahpintar/statistik-engine/internal/repository"
)

type StatistikService struct {
	repo *repository.StatistikRepo
}

func NewStatistikService(repo *repository.StatistikRepo) *StatistikService {
	return &StatistikService{repo: repo}
}

func (s *StatistikService) GetOverview(ctx context.Context) (*model.OverviewResult, error) {
	return s.repo.GetOverview(ctx)
}

func (s *StatistikService) GetAkademik(ctx context.Context, tahunAjaranID, kelasID *int64) (*model.AkademikResult, error) {
	return s.repo.GetAkademik(ctx, tahunAjaranID, kelasID)
}

func (s *StatistikService) GetKehadiran(ctx context.Context, tahunAjaranID, kelasID *int64, bulan, tahun int) (*model.KehadiranResult, error) {
	return s.repo.GetKehadiran(ctx, tahunAjaranID, kelasID, bulan, tahun)
}

func (s *StatistikService) GetKeuangan(ctx context.Context, tahun int) (*model.KeuanganResult, error) {
	return s.repo.GetKeuangan(ctx, tahun)
}

func (s *StatistikService) GetBK(ctx context.Context, tahun int, kelasID *int64) (*model.BKResult, error) {
	return s.repo.GetBK(ctx, tahun, kelasID)
}

func (s *StatistikService) GetPPDB(ctx context.Context, tahun int) (*model.PPDBResult, error) {
	return s.repo.GetPPDB(ctx, tahun)
}

func (s *StatistikService) GetPerpustakaan(ctx context.Context, tahun int) (*model.PerpustakaanResult, error) {
	return s.repo.GetPerpustakaan(ctx, tahun)
}

func (s *StatistikService) GetUjian(ctx context.Context, kelasID, mapelID *int64, semester string, kkm float64) (*model.UjianResult, error) {
	if kkm == 0 {
		kkm = 75.0 // default KKM
	}
	return s.repo.GetUjian(ctx, kelasID, mapelID, semester, kkm)
}

func (s *StatistikService) GetEkstrakurikuler(ctx context.Context, tahun int) (*model.EkstrakurikulerResult, error) {
	return s.repo.GetEkstrakurikuler(ctx, tahun)
}

func (s *StatistikService) GetOrganisasi(ctx context.Context) (*model.OrganisasiResult, error) {
	return s.repo.GetOrganisasi(ctx)
}

func (s *StatistikService) GetGuru(ctx context.Context, startDate, endDate string) (*model.GuruResult, error) {
	return s.repo.GetGuru(ctx, startDate, endDate)
}

func (s *StatistikService) GetSPK(ctx context.Context) (*model.SPKResult, error) {
	return s.repo.GetSPK(ctx)
}
