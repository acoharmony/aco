# Simple single-stage build using official Node.js image
FROM node:22-slim

LABEL org.opencontainers.image.source=https://github.com/acoharmony/acoharmony

# Set working directory
WORKDIR /home/care/acoharmony/docs

# Copy package files and pre-installed dependencies
# Note: Copying node_modules avoids npm install memory issues in constrained environments
COPY docs/package*.json ./
COPY docs/node_modules ./node_modules

# Create directory structure for volume mount points
RUN mkdir -p .docusaurus && \
    chown -R 1002:1002 /home/care

# Environment configuration
ENV HOME=/home/care \
    NODE_ENV=development \
    PATH="/home/care/acoharmony/docs/node_modules/.bin:${PATH}"

# Security: Run as non-root user
USER 1002:1002

# Expose Docusaurus port
EXPOSE 3000

# Start Docusaurus dev server
CMD ["npm", "start"]
