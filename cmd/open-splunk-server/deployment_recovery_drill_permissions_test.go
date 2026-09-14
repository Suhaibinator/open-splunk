//go:build linux

package main

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestRecoveryDrillAdministratorSeedIsolation(t *testing.T) {
	if os.Getenv("OPEN_SPLUNK_OCI_INTEGRATION") != "1" {
		t.Skip("set OPEN_SPLUNK_OCI_INTEGRATION=1 for the isolated UID filesystem probe")
	}
	dockerfile, err := os.ReadFile("../../Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	var image string
	for line := range strings.SplitSeq(string(dockerfile), "\n") {
		fields := strings.Fields(line)
		if len(fields) >= 3 && fields[0] == "FROM" && strings.HasPrefix(fields[2], "node:") {
			image = fields[2]
			break
		}
	}
	if !strings.Contains(image, "@sha256:") {
		t.Fatal("the repository must pin its Node image by digest")
	}
	// Use the exact drill initializer on a disposable filesystem, then read as
	// the server owner, ClickHouse (including its shared supplementary group),
	// and an unrelated UID. No host paths or real credentials enter the probe.
	const probe = `node <<'JS'
const fs = require('node:fs');
const { spawnSync } = require('node:child_process');
for (const [path, mode] of [['/config/administrator', 0o700], ['/config/administrator/seed', 0o444]]) {
  const info = fs.statSync(path);
  if (info.uid !== 65532 || info.gid !== 65532 || (info.mode & 0o7777) !== mode) throw Error('incorrect seed ownership or mode');
}
for (const [uid, allowed] of [[65532, true], [101, false], [1001, false]]) {
  const source = "const fs = require('node:fs'); process.setgroups([65532]); process.setgid(" + uid + "); process.setuid(" + uid + "); let readable = false; try { fs.readFileSync('/config/administrator/seed'); readable = true; } catch (error) { if (error.code !== 'EACCES') throw error; } if (readable !== " + allowed + ") throw Error('unexpected seed access');";
  const result = spawnSync(process.execPath, ['-e', source], { encoding: 'utf8' });
  if (result.status !== 0) throw Error('UID ' + uid + ': ' + result.stderr);
}
console.log('owner readable; ClickHouse and unrelated UID denied');
JS`
	script := "mkdir /config/administrator && printf fixture-token > /config/administrator/seed && " +
		recoveryDrillAdministratorSeedInitializationCommand() + "\n" + probe + "\n" +
		recoveryDrillAdministratorSeedCleanupCommand + "\ntest ! -e /config/administrator"
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "docker", "run", "--rm", "--pull=never", "--network", "none",
		"--read-only", "--user", "0:0", "--tmpfs", "/config:rw,nosuid,nodev,mode=0755",
		"--entrypoint", "sh", image, "-ec", script)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("administrator seed isolation probe: %v\n%s", err, output)
	} else {
		t.Log(strings.TrimSpace(string(output)))
	}
}
