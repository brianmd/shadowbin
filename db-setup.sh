#!/bin/bash
aql -c "
DELETE FROM ttd-user.User where PK=1;
INSERT INTO ttd-user.User (PK, XDeviceUsser2, G, UserMetadata, UserTarget2, AdFMap2) VALUES (1, 'xdev-bin', 'g-bin', 'meta-bin', 'target-bin', 'fmap-bin');
DELETE FROM ttd-fcap.f where PK=1;
"
./db-show.sh
