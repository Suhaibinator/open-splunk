package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"os"
	"strconv"
	"strings"

	gcconfig "github.com/Suhaibinator/go-common/pkg/config"
	"github.com/Suhaibinator/open-splunk/internal/objectstore"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

type recoveryObjectOptions struct {
	Endpoint, AccessKey, SecretKey, Bucket string
	UseTLS                                 bool
	configurationError                     error
}

func registerRecoveryObjectFlags(flags *flag.FlagSet, options *recoveryObjectOptions) {
	flags.StringVar(&options.Endpoint, "minio-endpoint", os.Getenv("OPEN_SPLUNK_SERVER_MINIO_ENDPOINT"), "private MinIO endpoint")
	flags.StringVar(&options.AccessKey, "minio-access-key", consumeRecoverySecret("OPEN_SPLUNK_SERVER_MINIO_ACCESS_KEY"), "private MinIO access identity")
	flags.StringVar(&options.SecretKey, "minio-secret-key", consumeRecoverySecret("OPEN_SPLUNK_SERVER_MINIO_SECRET_KEY"), "private MinIO secret; prefer environment")
	bucket := os.Getenv("OPEN_SPLUNK_SERVER_MINIO_BUCKET")
	if bucket == "" {
		bucket = "open-splunk-results"
	}
	flags.StringVar(&options.Bucket, "minio-bucket", bucket, "private versioned result bucket")
	useTLS := true
	if value, configured := os.LookupEnv("OPEN_SPLUNK_SERVER_MINIO_USE_TLS"); configured {
		var err error
		useTLS, err = strconv.ParseBool(value)
		if err != nil {
			options.configurationError = errors.New("OPEN_SPLUNK_SERVER_MINIO_USE_TLS must be a boolean")
		}
	}
	flags.BoolVar(&options.UseTLS, "minio-use-tls", useTLS, "use TLS for MinIO")
}

func consumeRecoverySecret(name string) string {
	value := os.Getenv(name)
	_ = os.Unsetenv(name)
	return value
}
func validateRecoveryDSN(dsn string) error {
	if strings.TrimSpace(dsn) == "" {
		return errors.New("-postgres-dsn is required")
	}
	if _, err := pgx.ParseConfig(dsn); err != nil {
		return errors.New("invalid PostgreSQL control connection string")
	}
	return nil
}
func (options recoveryObjectOptions) validate() error {
	if options.configurationError != nil {
		return options.configurationError
	}
	if strings.TrimSpace(options.Endpoint) == "" || strings.TrimSpace(options.AccessKey) == "" || options.SecretKey == "" || strings.TrimSpace(options.Bucket) == "" {
		return errors.New("private MinIO endpoint, access key, secret and bucket are required")
	}
	return nil
}

type recoveryObjects struct {
	searchartifacts.BlobStore
	io.Closer
}

func (options recoveryObjectOptions) open(ctx context.Context) (*recoveryObjects, error) {
	if err := options.validate(); err != nil {
		return nil, err
	}
	cfg := gcconfig.GetDefaults()
	cfg.MinioEndpoint = options.Endpoint
	cfg.MinioAccessKey = options.AccessKey
	cfg.MinioSecretKey = gcconfig.NewSecret(options.SecretKey)
	cfg.MinioBucket = options.Bucket
	cfg.MinioPublicBucket = ""
	cfg.MinioUseSSL = &options.UseTLS
	objects, err := objectstore.NewPrivateObjectStorage(ctx, gcconfig.Static(cfg).Persistence(), zap.NewNop())
	if err != nil {
		return nil, err
	}
	store, err := objectstore.NewContext(ctx, objects)
	if err != nil {
		return nil, errors.Join(err, objects.Close())
	}
	return &recoveryObjects{BlobStore: store, Closer: objects}, nil
}
