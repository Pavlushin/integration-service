package lkdb

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"onec-integration/internal/worksheets/usersreports"
)

type Storage struct {
	db           *sql.DB
	nextDatabase string
}

func NewStorage(db *sql.DB, nextDatabase string) (*Storage, error) {
	if db == nil {
		return nil, fmt.Errorf("lk mariadb pool is required")
	}
	if strings.TrimSpace(nextDatabase) == "" {
		nextDatabase = "sps_next"
	}

	return &Storage{
		db:           db,
		nextDatabase: nextDatabase,
	}, nil
}

func (s *Storage) GetLinks(ctx context.Context, locationCode1C *string) ([]usersreports.LinkRow, error) {
	query := `
		select
			l.id as link_id,
			l.name as link_name,
			l.change_id as link_change,
			locations.code_1c as location_code_1c
		from worksheets__links l
			join worksheets__work_tasks wt on l.id = wt.link_id
			join locations on locations.location_id = wt.object_id
	`

	args := make([]any, 0, 1)
	if locationCode1C != nil && *locationCode1C != "" {
		query += ` where locations.code_1c = ?`
		args = append(args, *locationCode1C)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query links: %w", err)
	}
	defer rows.Close()

	result := make([]usersreports.LinkRow, 0)
	for rows.Next() {
		var row usersreports.LinkRow
		if err := rows.Scan(&row.LinkID, &row.LinkName, &row.LinkChange, &row.LocationCode1C); err != nil {
			return nil, fmt.Errorf("scan link row: %w", err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate link rows: %w", err)
	}

	return result, nil
}

func (s *Storage) GetMastersInLink(ctx context.Context, locationCode1C *string, linkIDs []int) ([]usersreports.MasterRow, error) {
	query := `
		select
			e.code_1c as user_code_1c,
			sol.link_id,
			sol.index_list as master_index
		from worksheets__senior_of_link sol
			join employees e on e.profile_id = sol.user_id
			join worksheets__work_tasks wt on wt.link_id = sol.link_id
			join worksheets__objects o on o.id = wt.object_id
		where sol.status = 1
	`

	args := make([]any, 0, 1+len(linkIDs))
	if locationCode1C != nil && *locationCode1C != "" {
		query += ` and o.object_code1c = ?`
		args = append(args, *locationCode1C)
	}
	if len(linkIDs) > 0 {
		query += ` and sol.link_id in (` + placeholders(len(linkIDs)) + `)`
		for _, linkID := range linkIDs {
			args = append(args, linkID)
		}
	}
	query += ` order by sol.link_id`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query masters in link: %w", err)
	}
	defer rows.Close()

	result := make([]usersreports.MasterRow, 0)
	for rows.Next() {
		var row usersreports.MasterRow
		if err := rows.Scan(&row.UserCode1C, &row.LinkID, &row.MasterIndex); err != nil {
			return nil, fmt.Errorf("scan master row: %w", err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate master rows: %w", err)
	}

	return result, nil
}

func (s *Storage) GetLastInactiveMastersInLink(ctx context.Context, locationCode1C *string, linkIDs []int) ([]usersreports.MasterRow, error) {
	if len(linkIDs) == 0 {
		return nil, nil
	}

	query := `
		select
			e.code_1c as user_code_1c,
			sol.link_id,
			sol.index_list as master_index
		from worksheets__senior_of_link sol
			join (
				select max(inner_sol.id) as id
				from worksheets__senior_of_link inner_sol
					join worksheets__work_tasks inner_wt on inner_wt.link_id = inner_sol.link_id
					join worksheets__objects inner_o on inner_o.id = inner_wt.object_id
				where inner_sol.status <> 1
	`

	args := make([]any, 0, 1+len(linkIDs))
	if locationCode1C != nil && *locationCode1C != "" {
		query += ` and inner_o.object_code1c = ?`
		args = append(args, *locationCode1C)
	}
	query += ` and inner_sol.link_id in (` + placeholders(len(linkIDs)) + `)`
	for _, linkID := range linkIDs {
		args = append(args, linkID)
	}
	query += `
				group by inner_sol.link_id
			) last_sol on last_sol.id = sol.id
			join employees e on e.profile_id = sol.user_id
		order by sol.link_id
	`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query fallback masters in link: %w", err)
	}
	defer rows.Close()

	result := make([]usersreports.MasterRow, 0)
	for rows.Next() {
		var row usersreports.MasterRow
		if err := rows.Scan(&row.UserCode1C, &row.LinkID, &row.MasterIndex); err != nil {
			return nil, fmt.Errorf("scan fallback master row: %w", err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate fallback master rows: %w", err)
	}

	return result, nil
}

func (s *Storage) GetMainReports(ctx context.Context, dateFrom string, dateTo string, locationCode1C *string) ([]usersreports.MainReportRow, error) {
	query := `
		select
			r.id as report_id,
			r.link_id,
			r.date_start as report_date,
			r.status_report as report_status
		from worksheets__reports_by_task r
			join worksheets__work_tasks wt on r.link_id = wt.link_id
			join worksheets__objects o on wt.object_id = o.id
		where r.date_start >= ? and r.date_start <= ?
	`

	args := []any{dateFrom + " 00:00:00", dateTo + " 23:59:59"}
	if locationCode1C != nil && *locationCode1C != "" {
		query += ` and o.object_code1c = ?`
		args = append(args, *locationCode1C)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query main reports: %w", err)
	}
	defer rows.Close()

	result := make([]usersreports.MainReportRow, 0)
	for rows.Next() {
		var row usersreports.MainReportRow
		if err := rows.Scan(&row.ReportID, &row.LinkID, &row.ReportDate, &row.ReportStatus); err != nil {
			return nil, fmt.Errorf("scan main report row: %w", err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate main report rows: %w", err)
	}

	return result, nil
}

func (s *Storage) GetUserReports(ctx context.Context, employeeCode1C *string, reportIDs []int) ([]usersreports.UserReportRow, error) {
	filteredReportIDs := filterPositiveIDs(reportIDs)
	if len(filteredReportIDs) == 0 {
		return nil, nil
	}

	const chunkSize = 250
	result := make([]usersreports.UserReportRow, 0)
	for start := 0; start < len(filteredReportIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(filteredReportIDs) {
			end = len(filteredReportIDs)
		}

		rows, err := s.queryUserReportsChunk(ctx, employeeCode1C, filteredReportIDs[start:end])
		if err != nil {
			return nil, err
		}

		result = append(result, rows...)
	}

	return result, nil
}

func (s *Storage) queryUserReportsChunk(ctx context.Context, employeeCode1C *string, reportIDs []int) ([]usersreports.UserReportRow, error) {
	query := `
			select
				ur.report_id,
			ur.id,
			ur.link_id,
			ur.user_code1c,
			ur.status as report_status,
			ur.position_code1c as position_code_1c,
			coalesce(ur.vehicle_code_1c, '') as vehicle_code_1c,
			coalesce(ur.val_1c_confirm, 0) as val_1c_confirm,
			wt.work_name as time_type_name,
			wt.description as time_type_description,
				coalesce(ur.hours_count, 0) as master_hours,
				coalesce(ur.dispatcher_hours, 0) as dispatcher_hours
			from worksheets__users_reports ur
				join worksheets__work_Type wt on ur.time_type = wt.id
			where ur.report_id in (` + placeholders(len(reportIDs)) + `)
		`

	args := make([]any, 0, len(reportIDs)+1)
	for _, reportID := range reportIDs {
		args = append(args, reportID)
	}
	if employeeCode1C != nil && *employeeCode1C != "" {
		query += ` and ur.user_code1c = ?`
		args = append(args, *employeeCode1C)
	}
	query += ` order by ur.link_id`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query user reports: %w", err)
	}
	defer rows.Close()

	result := make([]usersreports.UserReportRow, 0)
	for rows.Next() {
		var row usersreports.UserReportRow
		if err := rows.Scan(
			&row.ReportID,
			&row.ID,
			&row.LinkID,
			&row.UserCode1C,
			&row.ReportStatus,
			&row.PositionCode1C,
			&row.VehicleCode1C,
			&row.Val1CConfirm,
			&row.TimeTypeName,
			&row.TimeTypeDescription,
			&row.MasterHours,
			&row.DispatcherHours,
		); err != nil {
			return nil, fmt.Errorf("scan user report row: %w", err)
		}
		result = append(result, row)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate user report rows: %w", err)
	}

	return result, nil
}

func (s *Storage) GetSheetFlagsByLocationCodes(ctx context.Context, locationCodes1C []string) (map[string]int, error) {
	locationCodes1C = uniqueNonEmptyStrings(locationCodes1C)
	if len(locationCodes1C) == 0 {
		return map[string]int{}, nil
	}

	query := `
		select location_code_1c
		from ` + s.nextDatabase + `.locations__properties
		where property_id = ?
			and location_code_1c in (` + placeholders(len(locationCodes1C)) + `)
	`

	args := make([]any, 0, 1+len(locationCodes1C))
	args = append(args, 1)
	for _, code := range locationCodes1C {
		args = append(args, code)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query sheet flags: %w", err)
	}
	defer rows.Close()

	result := make(map[string]int, len(locationCodes1C))
	for rows.Next() {
		var code string
		if err := rows.Scan(&code); err != nil {
			return nil, fmt.Errorf("scan sheet flag row: %w", err)
		}
		result[code] = 1
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate sheet flag rows: %w", err)
	}

	return result, nil
}

func placeholders(count int) string {
	if count <= 0 {
		return ""
	}

	parts := make([]string, count)
	for i := range count {
		parts[i] = "?"
	}

	return strings.Join(parts, ",")
}

func filterPositiveIDs(values []int) []int {
	result := make([]int, 0, len(values))
	for _, value := range values {
		if value > 0 {
			result = append(result, value)
		}
	}
	return result
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
