#!/usr/bin/env bash
cat build.gradle | perl -E 'if (join("", <>) =~ /version = "([^"]*)"/) { say $1 } else { exit 1 }'
