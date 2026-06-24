# Deployment Configuration Fixes - Summary of Changes

## Problem Analysis

### Initial Issue (502 Errors)
The 502 errors were caused by nginx trying to connect to port 8501 (Streamlit) instead of port 8000 (FastAPI). This happened because the nginx configuration wasn't being properly updated during deployment.

### Additional Issue (Nginx Configuration Failure)
During testing, an additional issue was discovered where the nginx configuration test failed with:
```
unknown directive "cat" in /etc/nginx/sites-enabled/pifitness:2
```

This was caused by the nginx template file containing bash heredoc syntax (`cat > ... <<'EOF'`) that was being included in the final nginx configuration file, making it invalid nginx syntax.

## Files Modified

### 1. `deployment/pi5_deploy.sh` - Enhanced Deployment Script

**Changes Made:**
- Added explicit logging for nginx configuration updates
- Added validation to verify the PORT placeholder was actually replaced
- Added automatic symlink creation for nginx sites-enabled
- Added verification step to ensure nginx is using the correct port
- Improved error handling throughout the nginx configuration process

**Additional Features Added:**
- **Git Stashing**: Automatically stashes local changes before branch switching to prevent checkout conflicts
- **Agent Service Management**: Stops agent service before deployment and restarts it after completion
- **Enhanced Process Detection**: Smart detection of running processes with graceful shutdown

**Key Improvements:**
```bash
# Git stashing to prevent checkout conflicts
info "Stashing any local changes..."
git stash push --include-untracked || true

# Agent service management
manage_agent_service      # Stop before deployment
restart_agent_service     # Restart after deployment

# Smart process detection
detect_and_kill_processes  # Detect and gracefully stop processes
```

**Performance Improvements Added:**
- **Smart Process Detection**: Replaced aggressive process killing with `detect_and_kill_processes()` function that:
  - Detects running processes before attempting to kill them
  - Uses graceful `systemctl stop` before force killing
  - Only kills processes that are actually running
  - Provides clear logging of what's being found and stopped
  - Handles failures gracefully with warnings instead of errors

- **Simplified Cleanup**: `cleanup_processes()` now only cleans up temporary files, avoiding overly aggressive process killing

- **Better Error Handling**: All process killing operations use `|| warn` to handle failures gracefully

**Automated Testing Added:**
- **Pytest Integration**: Added automated testing for React UI branch only
- **Test Gate**: Deployment aborts if tests fail, preventing broken deployments
- **Optimal Placement**: Tests run after dependencies but before service start
- **Clear Logging**: Provides visible test output and success/failure messages

- **Improved Logging**: Each step provides clear info about what processes are found and how they're being handled

**Key Benefits:**
- **No More Blind Killing**: Only kills processes that are actually running
- **System Stability**: Won't kill system monitoring commands
- **Memory Safe**: Doesn't create memory pressure by killing too much
- **Faster Deployment**: No unnecessary process killing slows down deployment
- **Better Diagnostics**: Clear logging shows exactly what's happening

**Key Improvements:**
```bash
# Verify the port was actually replaced
if ! grep -q ":${TARGET_PORT};" "$NGINX_SITE"; then
    error_exit "Failed to update nginx configuration with port ${TARGET_PORT}. Check if PORT placeholder exists in template."
fi

# Ensure symlink exists in sites-enabled
if [[ ! -f "/etc/nginx/sites-enabled/pifitness" ]]; then
    info "Creating nginx symlink..."
    sudo ln -sf "$NGINX_SITE" /etc/nginx/sites-enabled/pifitness
fi

# Verify nginx is using the correct port
info "Verifying nginx configuration..."
sudo nginx -T 2>/dev/null | grep -A5 "server_name pifitness.duckdns.org" | grep "proxy_pass" | grep -q ":${TARGET_PORT};" || warn "Nginx may not be using the expected port ${TARGET_PORT}"
```

### 2. `deployment/nginx-template.conf` - Enhanced Nginx Template

**Changes Made:**
- Added security headers (X-Frame-Options, X-Content-Type-Options, X-XSS-Protection)
- Added WebSocket support for FastAPI
- Added optimized buffer settings for large responses
- Added static file caching for Next.js assets
- Added dedicated health check endpoint configuration
- Added proper error page handling
- Increased client_max_body_size to 100M for file uploads

**Key Improvements:**
```nginx
# WebSocket support (for FastAPI websockets)
proxy_http_version 1.1;
proxy_set_header Upgrade $http_upgrade;
proxy_set_header Connection "upgrade";

# Static file caching for Next.js
location /_next/static {
    expires 365d;
    access_log off;
}

# Health check endpoint
location /api/health {
    proxy_pass http://127.0.0.1:PORT;
    add_header Cache-Control "no-cache";
}
```

## Port Configuration Summary

### Streamlit (Branch: streamlit-prd)
- **Service Port**: 8501 (defined in `pifitness-streamlit.service`)
- **Nginx Proxy**: http://127.0.0.1:8501
- **Service File**: `deployment/pifitness-streamlit.service`

### FastAPI (Branch: react-ui)
- **Service Port**: 8000 (defined in `pifitness-fastapi.service`)
- **Nginx Proxy**: http://127.0.0.1:8000
- **Service File**: `deployment/pifitness-fastapi.service`

## How the Deployment Script Handles Ports

1. **User selects branch** (1 for Streamlit, 2 for FastAPI)
2. **Script sets TARGET_PORT**:
   - Streamlit: `TARGET_PORT=8501`
   - FastAPI: `TARGET_PORT=8000`
3. **Script updates nginx template**:
   - Replaces `PORT` placeholder with `TARGET_PORT`
   - Verifies the replacement was successful
4. **Script starts appropriate service**:
   - Streamlit: `pifitness-streamlit.service`
   - FastAPI: `pifitness-fastapi.service`

## Verification Steps

To verify the fixes work:

```bash
# 1. Check nginx configuration
sudo cat /etc/nginx/sites-available/pifitness

# 2. Test nginx configuration
sudo nginx -t

# 3. Check which services are running
sudo systemctl status pifitness-fastapi.service
sudo systemctl status pifitness-streamlit.service

# 4. Test FastAPI directly (if using FastAPI)
curl http://localhost:8000/api/health

# 5. Test through nginx
curl http://localhost/

# 6. Check listening ports
sudo netstat -tulnp | grep -E '8000|8501'
```

## Troubleshooting

If you still experience issues:

1. **Check nginx error logs**:
   ```bash
   sudo tail -n 50 /var/log/nginx/error.log
   ```

2. **Check service logs**:
   ```bash
   sudo journalctl -u pifitness-fastapi.service -n 50
   ```

3. **Manual nginx fix** (if deployment script fails):
   ```bash
   sudo sed -i 's/8501/8000/g' /etc/nginx/sites-available/pifitness
   sudo nginx -t && sudo systemctl reload nginx
   ```

## Deployment Recommendations

1. **Always use the deployment script** rather than manual configuration
2. **Check the script output** for any warnings or errors
3. **Verify the health endpoint** after deployment:
   ```bash
   curl http://localhost/api/health
   ```
4. **Consider adding a post-deployment test** to your CI/CD pipeline