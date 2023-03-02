#!/bin/bash
aql -c "
SELECT * from ttd-user.User where PK=1;
SELECT * from ttd-fcap.f where PK=1;
"
