package config

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"github.com/sirupsen/logrus"
)

// Load reads and parses the config file at path.
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %q: %w", path, err)
	}
	return Parse(raw)
}

// Watch monitors path and invokes onChange with each successfully parsed
// version of the file. It blocks until ctx is cancelled. Parse/read errors are
// logged and skipped so a bad edit never tears the watcher down.
//
// The parent directory is watched (rather than the file itself) so that atomic
// saves — where an editor replaces the file via rename — are still detected.
func Watch(ctx context.Context, path string, log *logrus.Logger, onChange func(*Config)) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer watcher.Close()

	abs, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve path: %w", err)
	}
	dir := filepath.Dir(abs)
	if err := watcher.Add(dir); err != nil {
		return fmt.Errorf("watch %q: %w", dir, err)
	}

	log.WithField("path", abs).Info("watching config for changes")

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			eventAbs, _ := filepath.Abs(event.Name)
			if eventAbs != abs {
				continue
			}
			if event.Op&(fsnotify.Write|fsnotify.Create|fsnotify.Rename) == 0 {
				continue
			}
			cfg, err := Load(path)
			if err != nil {
				log.WithError(err).Warn("ignoring invalid config change")
				continue
			}
			log.Info("config reloaded")
			onChange(cfg)
		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			log.WithError(err).Warn("config watcher error")
		}
	}
}
