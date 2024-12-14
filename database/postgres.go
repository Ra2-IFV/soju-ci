package database

import (
	"context"
	"database/sql"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	promcollectors "github.com/prometheus/client_golang/prometheus/collectors"
	"gopkg.in/irc.v4"

	"codeberg.org/emersion/soju/xirc"
)

const postgresQueryTimeout = 5 * time.Second

const postgresConfigSchema = `
CREATE TABLE IF NOT EXISTS "Config" (
	id SMALLINT PRIMARY KEY,
	version INTEGER NOT NULL,
	CHECK(id = 1)
);
`

//go:embed postgres_schema.sql
var postgresSchema string

type PostgresDB struct {
	db   *sql.DB
	temp bool

	scopes     map[scopeKey]*scopeData
	scopesLock sync.Mutex
}

type scopeKey struct {
	addr    string
	target  string
	network int64 // discriminator used for private targets only
}

type scopeMessage struct {
	id  int64
	raw string
	t   time.Time
}

type scopeTargetData struct {
	id             int64
	rangeID        int64
	rangeEnd       int64
	rangeStartTime time.Time
	rangeEndTime   time.Time
	rangeHasText   bool
}

type scopeData struct {
	id       int64
	messages []scopeMessage
	targets  map[int64]*scopeTargetData // by Network.ID
	l        sync.Mutex
}

func OpenPostgresDB(source string) (Database, error) {
	sqlPostgresDB, err := sql.Open("postgres", source)
	if err != nil {
		return nil, err
	}

	// By default sql.DB doesn't have a connection limit. This can cause errors
	// because PostgreSQL has a default of 100 max connections.
	sqlPostgresDB.SetMaxOpenConns(25)

	db := &PostgresDB{
		db:     sqlPostgresDB,
		scopes: make(map[scopeKey]*scopeData),
	}
	if err := db.upgrade(); err != nil {
		sqlPostgresDB.Close()
		return nil, err
	}

	return db, nil
}

func openTempPostgresDB(source string) (*sql.DB, error) {
	db, err := sql.Open("postgres", source)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	// Store all tables in a temporary schema which will be dropped when the
	// connection to PostgreSQL is closed.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("SET search_path TO pg_temp"); err != nil {
		return nil, fmt.Errorf("failed to set PostgreSQL search_path: %v", err)
	}

	return db, nil
}

func OpenTempPostgresDB(source string) (Database, error) {
	sqlPostgresDB, err := openTempPostgresDB(source)
	if err != nil {
		return nil, err
	}

	db := &PostgresDB{
		db:     sqlPostgresDB,
		temp:   true,
		scopes: make(map[scopeKey]*scopeData),
	}
	if err := db.upgrade(); err != nil {
		sqlPostgresDB.Close()
		return nil, err
	}

	return db, nil
}

func (db *PostgresDB) template(t string) string {
	// Hack to convince postgres to lookup text search configurations in
	// pg_temp
	if db.temp {
		return strings.ReplaceAll(t, "@SCHEMA_PREFIX@", "pg_temp.")
	}
	return strings.ReplaceAll(t, "@SCHEMA_PREFIX@", "")
}

func (db *PostgresDB) upgrade() error {
	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(postgresConfigSchema); err != nil {
		return fmt.Errorf("failed to create Config table: %s", err)
	}

	var version int
	err = tx.QueryRow(`SELECT version FROM "Config"`).Scan(&version)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("failed to query schema version: %s", err)
	}

	if version == len(postgresMigrations) {
		return nil
	}
	if version > len(postgresMigrations) {
		return fmt.Errorf("soju (version %d) older than schema (version %d)", len(postgresMigrations), version)
	}

	if version == 0 {
		if _, err := tx.Exec(db.template(postgresSchema)); err != nil {
			return fmt.Errorf("failed to initialize schema: %s", err)
		}
	} else {
		for i := version; i < len(postgresMigrations); i++ {
			if _, err := tx.Exec(db.template(postgresMigrations[i])); err != nil {
				return fmt.Errorf("failed to execute migration #%v: %v", i, err)
			}
		}
	}

	_, err = tx.Exec(`INSERT INTO "Config" (id, version) VALUES (1, $1)
		ON CONFLICT (id) DO UPDATE SET version = $1`, len(postgresMigrations))
	if err != nil {
		return fmt.Errorf("failed to bump schema version: %v", err)
	}

	return tx.Commit()
}

func (db *PostgresDB) Close() error {
	return db.db.Close()
}

func (db *PostgresDB) RegisterMetrics(r prometheus.Registerer) error {
	if err := r.Register(&postgresMetricsCollector{db}); err != nil {
		return err
	}
	return r.Register(promcollectors.NewDBStatsCollector(db.db, "main"))
}

func (db *PostgresDB) Stats(ctx context.Context) (*DatabaseStats, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	var stats DatabaseStats
	row := db.db.QueryRowContext(ctx, `SELECT
		(SELECT COUNT(*) FROM "User") AS users,
		(SELECT COUNT(*) FROM "Network") AS networks,
		(SELECT COUNT(*) FROM "Channel") AS channels`)
	if err := row.Scan(&stats.Users, &stats.Networks, &stats.Channels); err != nil {
		return nil, err
	}

	return &stats, nil
}

func (db *PostgresDB) ListUsers(ctx context.Context) ([]User, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	rows, err := db.db.QueryContext(ctx,
		`SELECT id, username, password, admin, nick, realname, enabled,
			downstream_interacted_at, max_networks
		FROM "User"`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []User
	for rows.Next() {
		var user User
		var password, nick, realname sql.NullString
		var downstreamInteractedAt sql.NullTime
		if err := rows.Scan(&user.ID, &user.Username, &password, &user.Admin, &nick, &realname, &user.Enabled, &downstreamInteractedAt, &user.MaxNetworks); err != nil {
			return nil, err
		}
		user.Password = password.String
		user.Nick = nick.String
		user.Realname = realname.String
		user.DownstreamInteractedAt = downstreamInteractedAt.Time
		users = append(users, user)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return users, nil
}

func (db *PostgresDB) GetUser(ctx context.Context, username string) (*User, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	user := &User{Username: username}

	var password, nick, realname sql.NullString
	var downstreamInteractedAt sql.NullTime
	row := db.db.QueryRowContext(ctx,
		`SELECT id, password, admin, nick, realname, enabled, downstream_interacted_at, max_networks
		FROM "User"
		WHERE username = $1`,
		username)
	if err := row.Scan(&user.ID, &password, &user.Admin, &nick, &realname, &user.Enabled, &downstreamInteractedAt, &user.MaxNetworks); err != nil {
		return nil, err
	}
	user.Password = password.String
	user.Nick = nick.String
	user.Realname = realname.String
	user.DownstreamInteractedAt = downstreamInteractedAt.Time
	return user, nil
}

func (db *PostgresDB) ListInactiveUsernames(ctx context.Context, limit time.Time) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	rows, err := db.db.QueryContext(ctx,
		`SELECT username FROM "User" WHERE COALESCE(downstream_interacted_at, created_at) < $1`,
		limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var usernames []string
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			return nil, err
		}
		usernames = append(usernames, username)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return usernames, nil
}

func (db *PostgresDB) StoreUser(ctx context.Context, user *User) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	password := toNullString(user.Password)
	nick := toNullString(user.Nick)
	realname := toNullString(user.Realname)
	downstreamInteractedAt := toNullTime(user.DownstreamInteractedAt)

	var err error
	if user.ID == 0 {
		err = db.db.QueryRowContext(ctx, `
			INSERT INTO "User" (username, password, admin, nick, realname,
				enabled, downstream_interacted_at, max_networks)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
			RETURNING id`,
			user.Username, password, user.Admin, nick, realname, user.Enabled,
			downstreamInteractedAt, user.MaxNetworks).Scan(&user.ID)
	} else {
		_, err = db.db.ExecContext(ctx, `
			UPDATE "User"
			SET password = $1, admin = $2, nick = $3, realname = $4,
				enabled = $5, downstream_interacted_at = $6, max_networks = $7
			WHERE id = $8`,
			password, user.Admin, nick, realname, user.Enabled,
			downstreamInteractedAt, user.MaxNetworks, user.ID)
	}
	return err
}

func (db *PostgresDB) DeleteUser(ctx context.Context, id int64) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	_, err := db.db.ExecContext(ctx, `DELETE FROM "User" WHERE id = $1`, id)
	return err
}

func (db *PostgresDB) ListNetworks(ctx context.Context, userID int64) ([]Network, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	rows, err := db.db.QueryContext(ctx, `
		SELECT id, name, addr, nick, username, realname, certfp, pass, connect_commands, sasl_mechanism,
			sasl_plain_username, sasl_plain_password, sasl_external_cert, sasl_external_key, auto_away, enabled
		FROM "Network"
		WHERE "user" = $1`, userID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var networks []Network
	for rows.Next() {
		var net Network
		var name, nick, username, realname, certfp, pass, connectCommands sql.NullString
		var saslMechanism, saslPlainUsername, saslPlainPassword sql.NullString
		err := rows.Scan(&net.ID, &name, &net.Addr, &nick, &username, &realname, &certfp,
			&pass, &connectCommands, &saslMechanism, &saslPlainUsername, &saslPlainPassword,
			&net.SASL.External.CertBlob, &net.SASL.External.PrivKeyBlob, &net.AutoAway, &net.Enabled)
		if err != nil {
			return nil, err
		}
		net.Name = name.String
		net.Nick = nick.String
		net.Username = username.String
		net.Realname = realname.String
		net.CertFP = certfp.String
		net.Pass = pass.String
		if connectCommands.Valid {
			net.ConnectCommands = strings.Split(connectCommands.String, "\r\n")
		}
		net.SASL.Mechanism = saslMechanism.String
		net.SASL.Plain.Username = saslPlainUsername.String
		net.SASL.Plain.Password = saslPlainPassword.String
		networks = append(networks, net)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return networks, nil
}

func (db *PostgresDB) StoreNetwork(ctx context.Context, userID int64, network *Network) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	netName := toNullString(network.Name)
	nick := toNullString(network.Nick)
	netUsername := toNullString(network.Username)
	realname := toNullString(network.Realname)
	certfp := toNullString(network.CertFP)
	pass := toNullString(network.Pass)
	connectCommands := toNullString(strings.Join(network.ConnectCommands, "\r\n"))

	var saslMechanism, saslPlainUsername, saslPlainPassword sql.NullString
	if network.SASL.Mechanism != "" {
		saslMechanism = toNullString(network.SASL.Mechanism)
		switch network.SASL.Mechanism {
		case "PLAIN":
			saslPlainUsername = toNullString(network.SASL.Plain.Username)
			saslPlainPassword = toNullString(network.SASL.Plain.Password)
			network.SASL.External.CertBlob = nil
			network.SASL.External.PrivKeyBlob = nil
		case "EXTERNAL":
			// keep saslPlain* nil
		default:
			return fmt.Errorf("soju: cannot store network: unsupported SASL mechanism %q", network.SASL.Mechanism)
		}
	}

	var err error
	if network.ID == 0 {
		err = db.db.QueryRowContext(ctx, `
			INSERT INTO "Network" ("user", name, addr, nick, username, realname, certfp, pass, connect_commands,
				sasl_mechanism, sasl_plain_username, sasl_plain_password, sasl_external_cert,
				sasl_external_key, auto_away, enabled)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
			RETURNING id`,
			userID, netName, network.Addr, nick, netUsername, realname, certfp, pass, connectCommands,
			saslMechanism, saslPlainUsername, saslPlainPassword, network.SASL.External.CertBlob,
			network.SASL.External.PrivKeyBlob, network.AutoAway, network.Enabled).Scan(&network.ID)
	} else {
		_, err = db.db.ExecContext(ctx, `
			UPDATE "Network"
			SET name = $2, addr = $3, nick = $4, username = $5, realname = $6, certfp = $7, pass = $8,
				connect_commands = $9, sasl_mechanism = $10, sasl_plain_username = $11,
				sasl_plain_password = $12, sasl_external_cert = $13, sasl_external_key = $14,
				auto_away = $15, enabled = $16
			WHERE id = $1`,
			network.ID, netName, network.Addr, nick, netUsername, realname, certfp, pass, connectCommands,
			saslMechanism, saslPlainUsername, saslPlainPassword, network.SASL.External.CertBlob,
			network.SASL.External.PrivKeyBlob, network.AutoAway, network.Enabled)
	}
	return err
}

func (db *PostgresDB) DeleteNetwork(ctx context.Context, id int64) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	_, err := db.db.ExecContext(ctx, `DELETE FROM "Network" WHERE id = $1`, id)
	return err
}

func (db *PostgresDB) ListChannels(ctx context.Context, networkID int64) ([]Channel, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	rows, err := db.db.QueryContext(ctx, `
		SELECT id, name, key, detached, detached_internal_msgid, relay_detached, reattach_on, detach_after,
			detach_on
		FROM "Channel"
		WHERE network = $1`, networkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var channels []Channel
	for rows.Next() {
		var ch Channel
		var key, detachedInternalMsgID sql.NullString
		var detachAfter int64
		if err := rows.Scan(&ch.ID, &ch.Name, &key, &ch.Detached, &detachedInternalMsgID, &ch.RelayDetached, &ch.ReattachOn, &detachAfter, &ch.DetachOn); err != nil {
			return nil, err
		}
		ch.Key = key.String
		ch.DetachedInternalMsgID = detachedInternalMsgID.String
		ch.DetachAfter = time.Duration(detachAfter) * time.Second
		channels = append(channels, ch)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return channels, nil
}

func (db *PostgresDB) StoreChannel(ctx context.Context, networkID int64, ch *Channel) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	key := toNullString(ch.Key)
	detachAfter := int64(math.Ceil(ch.DetachAfter.Seconds()))

	var err error
	if ch.ID == 0 {
		err = db.db.QueryRowContext(ctx, `
			INSERT INTO "Channel" (network, name, key, detached, detached_internal_msgid, relay_detached, reattach_on,
				detach_after, detach_on)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			RETURNING id`,
			networkID, ch.Name, key, ch.Detached, toNullString(ch.DetachedInternalMsgID),
			ch.RelayDetached, ch.ReattachOn, detachAfter, ch.DetachOn).Scan(&ch.ID)
	} else {
		_, err = db.db.ExecContext(ctx, `
			UPDATE "Channel"
			SET name = $2, key = $3, detached = $4, detached_internal_msgid = $5,
				relay_detached = $6, reattach_on = $7, detach_after = $8, detach_on = $9
			WHERE id = $1`,
			ch.ID, ch.Name, key, ch.Detached, toNullString(ch.DetachedInternalMsgID),
			ch.RelayDetached, ch.ReattachOn, detachAfter, ch.DetachOn)
	}
	return err
}

func (db *PostgresDB) DeleteChannel(ctx context.Context, id int64) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	_, err := db.db.ExecContext(ctx, `DELETE FROM "Channel" WHERE id = $1`, id)
	return err
}

func (db *PostgresDB) ListDeliveryReceipts(ctx context.Context, networkID int64) ([]DeliveryReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	rows, err := db.db.QueryContext(ctx, `
		SELECT id, target, client, internal_msgid
		FROM "DeliveryReceipt"
		WHERE network = $1`, networkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var receipts []DeliveryReceipt
	for rows.Next() {
		var rcpt DeliveryReceipt
		if err := rows.Scan(&rcpt.ID, &rcpt.Target, &rcpt.Client, &rcpt.InternalMsgID); err != nil {
			return nil, err
		}
		receipts = append(receipts, rcpt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return receipts, nil
}

func (db *PostgresDB) StoreClientDeliveryReceipts(ctx context.Context, networkID int64, client string, receipts []DeliveryReceipt) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	tx, err := db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx,
		`DELETE FROM "DeliveryReceipt" WHERE network = $1 AND client = $2`,
		networkID, client)
	if err != nil {
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO "DeliveryReceipt" (network, target, client, internal_msgid)
		VALUES ($1, $2, $3, $4)
		RETURNING id`)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for i := range receipts {
		rcpt := &receipts[i]
		err := stmt.
			QueryRowContext(ctx, networkID, rcpt.Target, client, rcpt.InternalMsgID).
			Scan(&rcpt.ID)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (db *PostgresDB) GetReadReceipt(ctx context.Context, networkID int64, name string) (*ReadReceipt, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	receipt := &ReadReceipt{
		Target: name,
	}

	row := db.db.QueryRowContext(ctx,
		`SELECT id, timestamp FROM "ReadReceipt" WHERE network = $1 AND target = $2`,
		networkID, name)
	if err := row.Scan(&receipt.ID, &receipt.Timestamp); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	return receipt, nil
}

func (db *PostgresDB) StoreReadReceipt(ctx context.Context, networkID int64, receipt *ReadReceipt) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	var err error
	if receipt.ID != 0 {
		_, err = db.db.ExecContext(ctx, `
			UPDATE "ReadReceipt"
			SET timestamp = $1
			WHERE id = $2`,
			receipt.Timestamp, receipt.ID)
	} else {
		err = db.db.QueryRowContext(ctx, `
			INSERT INTO "ReadReceipt" (network, target, timestamp)
			VALUES ($1, $2, $3)
			RETURNING id`,
			networkID, receipt.Target, receipt.Timestamp).Scan(&receipt.ID)
	}
	return err
}

func (db *PostgresDB) listTopNetworkAddrs(ctx context.Context) (map[string]int, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	addrs := make(map[string]int)

	rows, err := db.db.QueryContext(ctx, `
		SELECT addr, COUNT(addr) AS n
		FROM "Network"
		GROUP BY addr
		ORDER BY n DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var (
			addr string
			n    int
		)
		if err := rows.Scan(&addr, &n); err != nil {
			return nil, err
		}
		addrs[addr] = n
	}

	return addrs, rows.Err()
}

func (db *PostgresDB) ListWebPushConfigs(ctx context.Context) ([]WebPushConfig, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	rows, err := db.db.QueryContext(ctx, `
		SELECT id, vapid_key_public, vapid_key_private
		FROM "WebPushConfig"`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []WebPushConfig
	for rows.Next() {
		var config WebPushConfig
		if err := rows.Scan(&config.ID, &config.VAPIDKeys.Public, &config.VAPIDKeys.Private); err != nil {
			return nil, err
		}
		configs = append(configs, config)
	}

	return configs, rows.Err()
}

func (db *PostgresDB) StoreWebPushConfig(ctx context.Context, config *WebPushConfig) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	if config.ID != 0 {
		return fmt.Errorf("cannot update a WebPushConfig")
	}

	err := db.db.QueryRowContext(ctx, `
		INSERT INTO "WebPushConfig" (created_at, vapid_key_public, vapid_key_private)
		VALUES (NOW(), $1, $2)
		RETURNING id`,
		config.VAPIDKeys.Public, config.VAPIDKeys.Private).Scan(&config.ID)
	return err
}

func (db *PostgresDB) ListWebPushSubscriptions(ctx context.Context, userID, networkID int64) ([]WebPushSubscription, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	nullNetworkID := sql.NullInt64{
		Int64: networkID,
		Valid: networkID != 0,
	}

	rows, err := db.db.QueryContext(ctx, `
		SELECT id, endpoint, created_at, updated_at, key_auth, key_p256dh, key_vapid
		FROM "WebPushSubscription"
		WHERE "user" = $1 AND network IS NOT DISTINCT FROM $2`, userID, nullNetworkID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var subs []WebPushSubscription
	for rows.Next() {
		var sub WebPushSubscription
		if err := rows.Scan(&sub.ID, &sub.Endpoint, &sub.CreatedAt, &sub.UpdatedAt, &sub.Keys.Auth, &sub.Keys.P256DH, &sub.Keys.VAPID); err != nil {
			return nil, err
		}
		subs = append(subs, sub)
	}

	return subs, rows.Err()
}

func (db *PostgresDB) StoreWebPushSubscription(ctx context.Context, userID, networkID int64, sub *WebPushSubscription) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	nullNetworkID := sql.NullInt64{
		Int64: networkID,
		Valid: networkID != 0,
	}

	var err error
	if sub.ID != 0 {
		_, err = db.db.ExecContext(ctx, `
			UPDATE "WebPushSubscription"
			SET updated_at = NOW(), key_auth = $1, key_p256dh = $2,
				key_vapid = $3
			WHERE id = $4`,
			sub.Keys.Auth, sub.Keys.P256DH, sub.Keys.VAPID, sub.ID)
	} else {
		err = db.db.QueryRowContext(ctx, `
			INSERT INTO "WebPushSubscription" (created_at, updated_at, "user",
				network, endpoint, key_auth, key_p256dh, key_vapid)
			VALUES (NOW(), NOW(), $1, $2, $3, $4, $5, $6)
			RETURNING id`,
			userID, nullNetworkID, sub.Endpoint, sub.Keys.Auth, sub.Keys.P256DH,
			sub.Keys.VAPID).Scan(&sub.ID)
	}

	return err
}

func (db *PostgresDB) DeleteWebPushSubscription(ctx context.Context, id int64) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	_, err := db.db.ExecContext(ctx, `DELETE FROM "WebPushSubscription" WHERE id = $1`, id)
	return err
}

func (db *PostgresDB) GetMessageLastID(ctx context.Context, networkID int64, name string) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	var msgID int64
	row := db.db.QueryRowContext(ctx, `
		SELECT m.id FROM "Message" m, "MessageRange" r
		WHERE
		r.target = (
			SELECT id FROM "MessageTarget"
			WHERE network = $1 AND target = $2
		)
		AND r.scope = m.scope AND m.id >= r.start AND m.id <= r."end"
		ORDER BY m.id DESC LIMIT 1`,
		networkID,
		name,
	)
	if err := row.Scan(&msgID); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, err
	}
	return msgID, nil
}

func (db *PostgresDB) GetMessageTarget(ctx context.Context, networkID int64, target string) (*MessageTarget, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	mt := &MessageTarget{
		Target: target,
	}

	row := db.db.QueryRowContext(ctx,
		`SELECT id, pinned, muted FROM "MessageTarget" WHERE network = $1 AND target = $2`,
		networkID, target)
	if err := row.Scan(&mt.ID, &mt.Pinned, &mt.Muted); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	return mt, nil
}

func (db *PostgresDB) StoreMessageTarget(ctx context.Context, networkID int64, mt *MessageTarget) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	var err error
	if mt.ID != 0 {
		_, err = db.db.ExecContext(ctx, `
			UPDATE "MessageTarget"
			SET pinned = $1, muted = $2
			WHERE id = $3`,
			mt.Pinned, mt.Muted, mt.ID)
	} else {
		err = db.db.QueryRowContext(ctx, `
			INSERT INTO "MessageTarget" (network, target, pinned, muted)
			VALUES ($1, $2, $3, $4)
			RETURNING id`,
			networkID, mt.Target, mt.Pinned, mt.Muted).Scan(&mt.ID)
	}
	return err
}

func (db *PostgresDB) StoreMessages(ctx context.Context, network *Network, name string, msgs []*irc.Message) ([]int64, error) {
	if len(msgs) == 0 {
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(ctx, time.Duration(len(msgs))*postgresQueryTimeout)
	defer cancel()

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	k := scopeKey{
		addr:   network.Addr,
		target: name,
	}
	if !strings.HasPrefix(name, "#") {
		// Heuristic: looks like a private message; do not share messages of this target across networks.
		// We use the network ID as a discriminator.
		k.network = network.ID
	}
	db.scopesLock.Lock()
	scope, ok := db.scopes[k]
	if !ok {
		scope = &scopeData{
			messages: make([]scopeMessage, 0, 4),
			targets:  make(map[int64]*scopeTargetData),
		}
		db.scopes[k] = scope
	}
	db.scopesLock.Unlock()
	scope.l.Lock()
	defer scope.l.Unlock()

	if scope.id == 0 {
		// First message in scope since restart: try inserting.
		err = tx.QueryRowContext(ctx, `
			INSERT INTO "MessageScope" (addr, target, network)
			VALUES ($1, $2, $3)
			ON CONFLICT DO NOTHING
			RETURNING id`,
			k.addr,
			k.target,
			toNullInt64(k.network),
		).Scan(&scope.id)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		} else if err == sql.ErrNoRows {
			// Scope already exists: get and cache ID.
			err = tx.QueryRowContext(ctx, `
				SELECT id FROM "MessageScope"
				WHERE addr = $1 AND target = $2 AND network IS NOT DISTINCT FROM $3`,
				k.addr,
				k.target,
				toNullInt64(k.network),
			).Scan(&scope.id)
			if err != nil {
				return nil, err
			}
		}
	}

	target, ok := scope.targets[network.ID]
	if !ok {
		target = &scopeTargetData{}
		scope.targets[network.ID] = target
	}
	if target.id == 0 {
		// First message in target since restart: try inserting.
		err = tx.QueryRowContext(ctx, `
			INSERT INTO "MessageTarget" (network, target)
			VALUES ($1, $2)
			ON CONFLICT DO NOTHING
			RETURNING id`,
			network.ID,
			name,
		).Scan(&target.id)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		} else if err == sql.ErrNoRows {
			// Target already exists: get and cache ID.
			err = tx.QueryRowContext(ctx, `
				SELECT id FROM "MessageTarget"
				WHERE network = $1 AND target = $2`,
				network.ID,
				name,
			).Scan(&target.id)
			if err != nil {
				return nil, err
			}
		}
	}

	insertStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO "Message" (scope, raw, time, sender, text)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id
	`)
	if err != nil {
		return nil, err
	}

	updateRangeStmt, err := tx.PrepareContext(ctx, `
		UPDATE "MessageRange"
		SET "end" = $1, start_time = $2, end_time = $3, has_text = $4
		WHERE id = $5
	`)
	if err != nil {
		return nil, err
	}

	insertRangeStmt, err := tx.PrepareContext(ctx, `
		INSERT INTO "MessageRange" (target, scope, start, "end", start_time, end_time, has_text)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id
	`)
	if err != nil {
		return nil, err
	}

	var pendingRangeBumpEnd int64

	ids := make([]int64, len(msgs))
	for i, msg := range msgs {
		var t time.Time
		if tag, ok := msg.Tags["time"]; ok {
			var err error
			t, err = time.Parse(xirc.ServerTimeLayout, tag)
			if err != nil {
				return nil, fmt.Errorf("failed to parse message time tag: %w", err)
			}
		} else {
			t = time.Now()
		}
		// Strip message time from raw (we already store it in the time column).
		msg = msg.Copy()
		delete(msg.Tags, "time")

		var text sql.NullString
		switch msg.Command {
		case "PRIVMSG", "NOTICE":
			if len(msg.Params) > 1 {
				text.Valid = true
				text.String = stripANSI(msg.Params[1])
			}
		}
		raw := xirc.Raw(msg)

		outdatedMessages := 0
		var prevID int64
		var current *scopeMessage
		const maxSkew = 5 * time.Second
		for i, m := range scope.messages {
			// Try to find an existing matching message in our cache.
			if m.t.Before(t.Add(-maxSkew)) {
				outdatedMessages++
				continue
			}
			if target.rangeEnd >= m.id {
				// Already added this message to this network: likely the same message sent multiple times; skip.
				continue
			}
			if m.raw != raw {
				continue
			}
			// Matching message: select.
			if i > 0 {
				prevID = scope.messages[i-1].id
			}
			current = &scope.messages[i]
			break
		}
		if current == nil {
			// Message not found in our cache: insert it.
			if len(scope.messages) > 0 {
				prevID = scope.messages[len(scope.messages)-1].id
			}
			scope.messages = append(scope.messages, scopeMessage{
				raw: raw,
				t:   t,
			})
			current = &scope.messages[len(scope.messages)-1]

			err = insertStmt.QueryRowContext(ctx, scope.id, raw, t, msg.Name, text).Scan(&current.id)
			if err != nil {
				return nil, err
			}
		}
		ids[i] = current.id

		if prevID == 0 {
			// Previous message in the scope not in cache; fetch it.
			err = tx.QueryRowContext(ctx, `
				SELECT m.id FROM "Message" m
				WHERE m.scope = $1 AND m.id < $2
				ORDER BY id DESC
				LIMIT 1
			`, scope.id, current.id).Scan(&prevID)
			if err != nil && err != sql.ErrNoRows {
				return nil, err
			}
		}

		if target.rangeEnd == 0 {
			// Last message of target not in cache; fetch it.
			err = tx.QueryRowContext(ctx, `
				SELECT id, "end", start_time, end_time, has_text FROM "MessageRange"
				WHERE target = $1
				ORDER BY "end" DESC
				LIMIT 1
			`, target.id).Scan(&target.rangeID, &target.rangeEnd, &target.rangeStartTime, &target.rangeEndTime, &target.rangeHasText)
			if err != nil && err != sql.ErrNoRows {
				return nil, err
			}
		}

		// If the last message of target is the message before the newly stored one;
		// or if the last message is the current one (noop; can happen after joining a target with -share-history).
		contiguousRange := target.rangeEnd != 0 && (target.rangeEnd == prevID || target.rangeEnd == current.id)

		if !contiguousRange && pendingRangeBumpEnd != 0 {
			// We previously remembered to bump the end of range. Update it before inserting a new range.
			_, err = updateRangeStmt.ExecContext(ctx, pendingRangeBumpEnd, target.rangeStartTime, target.rangeEndTime, target.rangeHasText, target.rangeID)
			if err != nil {
				return nil, err
			}
			pendingRangeBumpEnd = 0
		}

		if !contiguousRange || target.rangeStartTime.IsZero() || current.t.Before(target.rangeStartTime) {
			target.rangeStartTime = current.t
		}
		if !contiguousRange || target.rangeEndTime.IsZero() || current.t.After(target.rangeEndTime) {
			target.rangeEndTime = current.t
		}
		target.rangeHasText = (contiguousRange && target.rangeHasText) || text.String != ""

		if contiguousRange {
			// Last message of target is the message before the newly stored one: remember to bump end of range.
			// This effectively caches/"collapses" multiple UPDATEs of that range (until we have no more messages to store, or we have to insert a new range).
			pendingRangeBumpEnd = current.id
		} else {
			// Last message of target is not contiguous with new message: add new range.
			err := insertRangeStmt.QueryRowContext(ctx, target.id, scope.id, current.id, current.id, target.rangeStartTime, target.rangeEndTime, target.rangeHasText).Scan(&target.rangeID)
			if err != nil {
				return nil, err
			}
		}
		target.rangeEnd = current.id

		const maxOutdatedMessages = 5
		if outdatedMessages > maxOutdatedMessages {
			// Drop some outdated messages from cache when too many are stored.
			copy(scope.messages, scope.messages[outdatedMessages-maxOutdatedMessages:])
			scope.messages = scope.messages[:len(scope.messages)-(outdatedMessages-maxOutdatedMessages)]
		}
	}

	if pendingRangeBumpEnd != 0 {
		// We previously remembered to bump the end of range. Update it now before committing the transaction.
		_, err = updateRangeStmt.ExecContext(ctx, pendingRangeBumpEnd, target.rangeStartTime, target.rangeEndTime, target.rangeHasText, target.rangeID)
		if err != nil {
			return nil, err
		}
	}

	err = tx.Commit()
	return ids, err
}

func (db *PostgresDB) ListMessageLastPerTarget(ctx context.Context, networkID int64, options *MessageOptions) ([]MessageTargetLast, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	parameters := []interface{}{
		networkID,
	}
	query := `
		SELECT t.target, l.latest
		FROM "MessageTarget" t JOIN LATERAL (
			SELECT r.target, r.end_time AS latest
			FROM "MessageRange" r
			WHERE r.target = t.id
	`

	if !options.Events {
		query += `AND r.has_text `
	}

	query += `
			ORDER BY r.end_time DESC LIMIT 1
		) AS l ON t.id = l.target
		WHERE t.network = $1
	`

	if !options.AfterTime.IsZero() {
		// compares time strings by lexicographical order
		parameters = append(parameters, options.AfterTime)
		query += fmt.Sprintf(`AND l.latest > $%d `, len(parameters))
	}
	if !options.BeforeTime.IsZero() {
		// compares time strings by lexicographical order
		parameters = append(parameters, options.BeforeTime)
		query += fmt.Sprintf(`AND l.latest < $%d `, len(parameters))
	}
	if options.TakeLast {
		query += `ORDER BY l.latest DESC `
	} else {
		query += `ORDER BY l.latest ASC `
	}
	parameters = append(parameters, options.Limit)
	query += fmt.Sprintf(`LIMIT $%d`, len(parameters))

	rows, err := db.db.QueryContext(ctx, query, parameters...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var l []MessageTargetLast
	for rows.Next() {
		var mt MessageTargetLast
		if err := rows.Scan(&mt.Name, &mt.LatestMessage); err != nil {
			return nil, err
		}

		l = append(l, mt)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if options.TakeLast {
		// We ordered by DESC to limit to the last lines.
		// Reverse the list to order by ASC these last lines.
		for i, j := 0, len(l)-1; i < j; i, j = i+1, j-1 {
			l[i], l[j] = l[j], l[i]
		}
	}

	return l, nil
}

func (db *PostgresDB) ListMessages(ctx context.Context, networkID int64, name string, options *MessageOptions) ([]*irc.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	queryMessageRange := `
			SELECT scope, "start", "end" FROM "MessageRange"
			WHERE target = (
				SELECT id FROM "MessageTarget"
				WHERE network = $1 AND target = $2
			)
	`
	queryMessage := `
			SELECT id, raw, time
			FROM "Message"
			WHERE r.scope = scope AND id >= r.start AND id <= r.end
	`

	parameters := []interface{}{
		networkID,
		name,
	}
	if !options.AfterTime.IsZero() {
		var startID int64
		err := db.db.QueryRowContext(ctx, `
			SELECT m.id FROM "Message" m
			WHERE m.scope = (
				SELECT scope FROM "MessageRange"
				WHERE target = (
					SELECT id FROM "MessageTarget"
					WHERE network = $1 AND target = $2
				) AND end_time > $3
				ORDER BY id ASC
				LIMIT 1
			) AND m.time > $4
			ORDER BY time ASC
			LIMIT 1
		`, networkID, name, options.AfterTime, options.AfterTime).Scan(&startID)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		} else if err == sql.ErrNoRows {
			return nil, nil
		}
		parameters = append(parameters, startID)
		queryMessageRange += fmt.Sprintf(`AND "end" >= $%d `, len(parameters))
		queryMessage += fmt.Sprintf(`AND id >= $%d `, len(parameters))
	}
	if !options.BeforeTime.IsZero() {
		var endID int64
		err := db.db.QueryRowContext(ctx, `
			SELECT m.id FROM "Message" m
			WHERE m.scope = (
				SELECT scope FROM "MessageRange"
				WHERE target = (
					SELECT id FROM "MessageTarget"
					WHERE network = $1 AND target = $2
				) AND start_time < $3
				ORDER BY id DESC
				LIMIT 1
			) AND m.time < $4
			ORDER BY time DESC
			LIMIT 1
		`, networkID, name, options.BeforeTime, options.BeforeTime).Scan(&endID)
		if err != nil && err != sql.ErrNoRows {
			return nil, err
		} else if err == sql.ErrNoRows {
			return nil, nil
		}
		parameters = append(parameters, endID)
		queryMessageRange += fmt.Sprintf(`AND start <= $%d `, len(parameters))
		queryMessage += fmt.Sprintf(`AND id <= $%d `, len(parameters))
	}
	if options.AfterID > 0 {
		parameters = append(parameters, options.AfterID)
		queryMessage += fmt.Sprintf(`AND id > $%d `, len(parameters))
	}
	if options.Sender != "" {
		parameters = append(parameters, options.Sender)
		queryMessage += fmt.Sprintf(`AND sender = $%d `, len(parameters))
	}
	if options.Text != "" {
		parameters = append(parameters, options.Text)
		queryMessage += fmt.Sprintf(`AND text_search @@ plainto_tsquery('search_simple', $%d) `, len(parameters))
	}
	if !options.Events {
		queryMessage += `AND text IS NOT NULL `
	}
	if options.TakeLast {
		queryMessage += `ORDER BY id DESC `
	} else {
		queryMessage += `ORDER BY id ASC `
	}
	// Limit each Message sub-query to the total limit.
	parameters = append(parameters, options.Limit)
	queryMessage += fmt.Sprintf(`LIMIT $%d `, len(parameters))

	query := fmt.Sprintf(`
		SELECT m.raw, m.time FROM
		(
			%s
		) AS r
		JOIN LATERAL
		(
			%s
		) AS m ON true
	`, queryMessageRange, queryMessage)

	if options.TakeLast {
		query += `ORDER BY r."start" DESC, m.id DESC `
	} else {
		query += `ORDER BY r."start" ASC, m.id ASC `
	}
	parameters = append(parameters, options.Limit)
	query += fmt.Sprintf(`LIMIT $%d`, len(parameters))

	rows, err := db.db.QueryContext(ctx, query, parameters...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var l []*irc.Message
	for rows.Next() {
		var raw string
		var t time.Time
		if err := rows.Scan(&raw, &t); err != nil {
			return nil, err
		}

		msg, err := irc.ParseMessage(raw)
		if err != nil {
			return nil, err
		}
		if _, ok := msg.Tags["time"]; !ok {
			msg.Tags["time"] = xirc.FormatServerTime(t)
		}

		l = append(l, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if options.TakeLast {
		// We ordered by DESC to limit to the last lines.
		// Reverse the list to order by ASC these last lines.
		for i, j := 0, len(l)-1; i < j; i, j = i+1, j-1 {
			l[i], l[j] = l[j], l[i]
		}
	}

	return l, nil
}

var postgresNetworksTotalDesc = prometheus.NewDesc("soju_networks_total", "Number of networks", []string{"hostname"}, nil)

type postgresMetricsCollector struct {
	db *PostgresDB
}

var _ prometheus.Collector = (*postgresMetricsCollector)(nil)

func (c *postgresMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- postgresNetworksTotalDesc
}

func (c *postgresMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	addrs, err := c.db.listTopNetworkAddrs(context.TODO())
	if err != nil {
		ch <- prometheus.NewInvalidMetric(postgresNetworksTotalDesc, err)
		return
	}

	// Group by hostname
	hostnames := make(map[string]int)
	for addr, n := range addrs {
		hostname := addr
		network := Network{Addr: addr}
		if u, err := network.URL(); err == nil {
			hostname = u.Hostname()
		}
		hostnames[hostname] += n
	}

	// Group networks with low counts for privacy
	watermark := 10
	grouped := 0
	for hostname, n := range hostnames {
		if n >= watermark && hostname != "" && hostname != "*" {
			ch <- prometheus.MustNewConstMetric(postgresNetworksTotalDesc, prometheus.GaugeValue, float64(n), hostname)
		} else {
			grouped += n
		}
	}
	if grouped > 0 {
		ch <- prometheus.MustNewConstMetric(postgresNetworksTotalDesc, prometheus.GaugeValue, float64(grouped), "*")
	}
}
