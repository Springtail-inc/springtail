#!/bin/bash
if [ ! -f /tmp/tmp.entitlements ]; then
    /usr/libexec/PlistBuddy -c "Add :com.apple.security.get-task-allow bool true" /tmp/tmp.entitlements
fi
codesign -s - -f --entitlements /tmp/tmp.entitlements $1
