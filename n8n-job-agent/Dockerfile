# Step 1: Grab a fresh Alpine image just to borrow its package manager
FROM alpine:latest AS alpine_base

# Step 2: Use the official n8n image
FROM n8nio/n8n:latest

USER root

# Step 3: Copy the package manager (apk) and its libraries from our Alpine base into n8n
COPY --from=alpine_base /sbin/apk /sbin/apk
COPY --from=alpine_base /usr/lib/libapk.so* /usr/lib/

# Step 4: Now we can successfully use apk to install Python and Requests!
RUN apk add --update --no-cache python3 py3-pip py3-requests

# Step 5: Switch back to the n8n node user for security
USER node