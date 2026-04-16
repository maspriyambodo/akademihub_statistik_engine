package middleware

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"strings"

	"github.com/golang-jwt/jwt/v5"
	"github.com/jmoiron/sqlx"
	"github.com/sekolahpintar/statistik-engine/internal/model"
)

type contextKey string

const claimsKey contextKey = "user_claims"

// Auth validates the Bearer JWT token and loads user claims into context.
func Auth(jwtSecret string, db *sqlx.DB) func(http.Handler) http.Handler {
	secretBytes := []byte(jwtSecret)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" || !strings.HasPrefix(authHeader, "Bearer ") {
				http.Error(w, `{"success":false,"message":"Unauthenticated"}`, http.StatusUnauthorized)
				return
			}

			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")

			token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
				if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, fmt.Errorf("unexpected signing method: %v", t.Header["alg"])
				}
				return secretBytes, nil
			}, jwt.WithExpirationRequired())

			if err != nil || !token.Valid {
				http.Error(w, `{"success":false,"message":"Token invalid or expired"}`, http.StatusUnauthorized)
				return
			}

			mapClaims, ok := token.Claims.(jwt.MapClaims)
			if !ok {
				http.Error(w, `{"success":false,"message":"Invalid token claims"}`, http.StatusUnauthorized)
				return
			}

			subStr, _ := mapClaims["sub"].(string)
			if subStr == "" {
				http.Error(w, `{"success":false,"message":"Invalid token sub"}`, http.StatusUnauthorized)
				return
			}

			var userID int64
			fmt.Sscanf(subStr, "%d", &userID)
			if userID == 0 {
				http.Error(w, `{"success":false,"message":"Invalid token sub"}`, http.StatusUnauthorized)
				return
			}

			claims, err := loadUserClaims(r.Context(), db, userID)
			if err != nil {
				http.Error(w, `{"success":false,"message":"Unauthenticated"}`, http.StatusUnauthorized)
				return
			}

			ctx := context.WithValue(r.Context(), claimsKey, claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// GetClaims retrieves UserClaims from request context.
func GetClaims(ctx context.Context) *model.UserClaims {
	c, _ := ctx.Value(claimsKey).(*model.UserClaims)
	return c
}

func loadUserClaims(ctx context.Context, db *sqlx.DB, userID int64) (*model.UserClaims, error) {
	var active int8
	err := db.QueryRowContext(ctx,
		db.Rebind(`SELECT is_active FROM sys_users WHERE id = ? AND deleted_at IS NULL`),
		userID,
	).Scan(&active)
	if err == sql.ErrNoRows || active == 0 {
		return nil, fmt.Errorf("user not found or inactive")
	}
	if err != nil {
		return nil, err
	}

	rows, err := db.QueryContext(ctx, db.Rebind(`
		SELECT r.name
		FROM sys_user_roles ur
		JOIN sys_roles r ON r.id = ur.sys_role_id
		WHERE ur.sys_user_id = ? AND ur.deleted_at IS NULL
	`), userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var roles []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, err
		}
		roles = append(roles, role)
	}

	claims := &model.UserClaims{UserID: userID, Roles: roles}

	var siswaID int64
	if err = db.QueryRowContext(ctx,
		db.Rebind(`SELECT id FROM mst_siswa WHERE sys_user_id = ? AND deleted_at IS NULL LIMIT 1`),
		userID,
	).Scan(&siswaID); err == nil {
		claims.SiswaID = &siswaID
	}

	var guruID int64
	if err = db.QueryRowContext(ctx,
		db.Rebind(`SELECT id FROM mst_guru WHERE sys_user_id = ? AND deleted_at IS NULL LIMIT 1`),
		userID,
	).Scan(&guruID); err == nil {
		claims.GuruID = &guruID
	}

	return claims, nil
}
