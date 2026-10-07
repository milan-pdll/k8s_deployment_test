package envflag

import (
	"flag"
	"os"
	"testing"
	"time"
)

// withFreshFlagSet swaps flag.CommandLine for a fresh one for the duration
// of a test, since String/Int/Duration/Bool all register against the
// package-level default FlagSet (flag.String et al.) -- without this,
// registering the same flag name in two tests would panic ("flag
// redefined").
func withFreshFlagSet(t *testing.T) {
	t.Helper()
	old := flag.CommandLine
	flag.CommandLine = flag.NewFlagSet(t.Name(), flag.ContinueOnError)
	t.Cleanup(func() { flag.CommandLine = old })
}

func TestEnvName_DashesToUnderscoresUpperCased(t *testing.T) {
	cases := map[string]string{
		"database-url":              "DATABASE_URL",
		"max-concurrent-activities": "MAX_CONCURRENT_ACTIVITIES",
		"s3-bucket":                 "S3_BUCKET",
		"timeout":                   "TIMEOUT",
		"":                          "",
		"already-UPPER-mixed-Case":  "ALREADY_UPPER_MIXED_CASE",
	}
	for flagName, want := range cases {
		if got := envName(flagName); got != want {
			t.Errorf("envName(%q) = %q, want %q", flagName, got, want)
		}
	}
}

func TestString_EnvVarOverridesDefault(t *testing.T) {
	withFreshFlagSet(t)
	t.Setenv("DATABASE_URL", "postgres://from-env")

	v := String("database-url", "postgres://default", "usage")
	if err := flag.CommandLine.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != "postgres://from-env" {
		t.Errorf("value = %q, want the env var's value", *v)
	}
}

func TestString_FallsBackToDefaultWhenEnvUnset(t *testing.T) {
	withFreshFlagSet(t)
	if err := os.Unsetenv("SOME_UNSET_FLAG"); err != nil {
		t.Fatal(err)
	}

	v := String("some-unset-flag", "the-default", "usage")
	if err := flag.CommandLine.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != "the-default" {
		t.Errorf("value = %q, want the hardcoded default", *v)
	}
}

func TestString_CLIFlagWinsOverEnvVar(t *testing.T) {
	withFreshFlagSet(t)
	t.Setenv("DATABASE_URL", "postgres://from-env")

	v := String("database-url", "postgres://default", "usage")
	if err := flag.CommandLine.Parse([]string{"--database-url=postgres://from-cli"}); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != "postgres://from-cli" {
		t.Errorf("value = %q, want the explicit CLI flag to win over both the env var and the default", *v)
	}
}

func TestInt_EnvVarOverridesDefault(t *testing.T) {
	withFreshFlagSet(t)
	t.Setenv("MAX_PAGES", "500")

	v := Int("max-pages", 100, "usage")
	if err := flag.CommandLine.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != 500 {
		t.Errorf("value = %d, want 500", *v)
	}
}

func TestInt_UnparseableEnvVarFallsBackToDefault(t *testing.T) {
	withFreshFlagSet(t)
	t.Setenv("MAX_PAGES", "not-a-number")

	v := Int("max-pages", 100, "usage")
	if err := flag.CommandLine.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != 100 {
		t.Errorf("value = %d, want the default (100), since the env var isn't a valid int", *v)
	}
}

func TestDuration_EnvVarOverridesDefault(t *testing.T) {
	withFreshFlagSet(t)
	t.Setenv("TIMEOUT", "30s")

	v := Duration("timeout", 10*time.Second, "usage")
	if err := flag.CommandLine.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != 30*time.Second {
		t.Errorf("value = %v, want 30s", *v)
	}
}

func TestDuration_UnparseableEnvVarFallsBackToDefault(t *testing.T) {
	withFreshFlagSet(t)
	t.Setenv("TIMEOUT", "not-a-duration")

	v := Duration("timeout", 10*time.Second, "usage")
	if err := flag.CommandLine.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != 10*time.Second {
		t.Errorf("value = %v, want the default (10s)", *v)
	}
}

func TestBool_EnvVarOverridesDefault(t *testing.T) {
	withFreshFlagSet(t)
	t.Setenv("SAME_HOST_ONLY", "false")

	v := Bool("same-host-only", true, "usage")
	if err := flag.CommandLine.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != false {
		t.Errorf("value = %v, want false", *v)
	}
}

func TestBool_UnparseableEnvVarFallsBackToDefault(t *testing.T) {
	withFreshFlagSet(t)
	t.Setenv("SAME_HOST_ONLY", "not-a-bool")

	v := Bool("same-host-only", true, "usage")
	if err := flag.CommandLine.Parse(nil); err != nil {
		t.Fatalf("Parse: %v", err)
	}

	if *v != true {
		t.Errorf("value = %v, want the default (true)", *v)
	}
}
