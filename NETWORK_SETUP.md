# Duels Network Setup Guide

This guide explains how to set up the Duels plugin for cross-server functionality using Velocity proxy.

## Architecture Overview

The network setup consists of:
- **Velocity Proxy**: Runs the `duels-velocity` plugin for cross-server coordination
- **Spigot/Paper Servers**: Run the main `duels-plugin` with network features enabled  
- **Redis**: Provides shared data storage and real-time communication

## Prerequisites

1. **Velocity Proxy Server** (3.3.0 or higher)
2. **Multiple Spigot/Paper Servers** (1.20.1 or higher)
3. **Redis Server** (6.0 or higher)
4. All servers must be able to connect to Redis

## Installation Steps

### 1. Redis Setup

Install and configure Redis on a server accessible by all your Minecraft servers:

```bash
# Install Redis (Ubuntu/Debian)
sudo apt update
sudo apt install redis-server

# Start Redis
sudo systemctl start redis-server
sudo systemctl enable redis-server

# Configure Redis (optional)
sudo nano /etc/redis/redis.conf
```

### 2. Velocity Plugin Installation

1. Download `duels-velocity-{version}.jar` 
2. Place it in your Velocity `plugins/` folder
3. Start Velocity to generate the config
4. Edit `plugins/duels-velocity/config.json`:

```json
{
  "redis": {
    "host": "your-redis-host",
    "port": 6379,
    "password": "",
    "database": 0,
    "prefix": "duels:"
  },
  "servers": {
    "duel-servers": ["duel1", "duel2", "duel3"],
    "default-duel-server": "duel1"
  },
  "network": {
    "debug-mode": false,
    "timeout-seconds": 30
  }
}
```

### 3. Spigot/Paper Plugin Configuration

1. Install the main Duels plugin on each server
2. Edit `plugins/Duels/config.yml` on each server:

```yaml
# Network configuration
network:
  # Enable cross-server functionality
  enabled: true
  
  # Unique server name (must match Velocity config)
  server-name: 'duel1'  # Change this for each server
  
  # Redis connection details
  redis:
    host: 'your-redis-host'
    port: 6379
    password: ''
    database: 0
  
  # Enable debug logging
  debug-mode: false
```

### 4. Server-Specific Setup

Each server should have a unique `server-name` in the network config:

- **duel1**: Primary duel server with most arenas
- **duel2**: Secondary duel server  
- **lobby**: Lobby server (can have queues but no arenas)
- **survival**: Survival server (can have queues but no arenas)

## Features

### Cross-Server Queue System

Players can join queues from any server:
- `/queue` - Opens queue GUI showing all available queues network-wide
- Players are automatically transferred to the appropriate duel server when matched
- Queue status is synchronized across all servers in real-time

### Arena Management

Arenas are server-specific but visible network-wide:
- Each server manages its own arenas
- Arena availability is synchronized across the network
- Players are transferred to the correct server for their duel

### Player Commands

All existing commands work across the network:
- `/duel <player>` - Shows if player is on a different server
- `/spectate` - Can spectate matches on any server (with transfer)
- `/party duel` - Works across servers for party members

### Admin Commands

Network-aware admin commands:
- `/duels reload` - Reloads network configuration
- `/duels arenas` - Shows arenas from all servers
- `/duels queues` - Shows network-wide queue status

## Network Status

Check network connectivity with:
```
/duels network status
```

This shows:
- Redis connection status
- Connected servers
- Network-wide player count
- Arena availability across servers

## Troubleshooting

### Common Issues

1. **Redis Connection Failed**
   - Check Redis server is running
   - Verify network connectivity
   - Check firewall settings

2. **Players Not Transferring**
   - Ensure server names match between Velocity and plugin configs
   - Check Velocity has permission to transfer players
   - Verify target server is online

3. **Arena Sync Issues**
   - Check Redis prefix is consistent
   - Verify network.enabled is true on all servers
   - Check for duplicate server names

### Debug Mode

Enable debug mode for detailed logging:
```yaml
network:
  debug-mode: true
```

This logs:
- Redis operations
- Player transfers
- Arena synchronization
- Network events

### Log Files

Check these log files for issues:
- Velocity: `logs/latest.log`
- Spigot servers: `logs/latest.log`
- Redis: `/var/log/redis/redis-server.log`

## Performance Considerations

### Redis Optimization

For high-traffic networks:
```conf
# /etc/redis/redis.conf
maxmemory 256mb
maxmemory-policy allkeys-lru
tcp-keepalive 60
```

### Network Bandwidth

The plugin uses minimal bandwidth:
- Player transfers: ~1KB per transfer
- Arena sync: ~2KB per arena update
- Queue updates: ~0.5KB per queue action

### Server Resources

Network features add minimal overhead:
- RAM: +10-20MB per server
- CPU: <1% additional usage
- Disk: Negligible (only config files)

## Migration from Single-Server

To migrate an existing single-server setup:

1. **Backup everything**
2. Set up Redis and Velocity plugin
3. Configure network settings on existing server
4. Test with `network.enabled: false` first
5. Enable network mode when ready
6. Add additional servers as needed

## Security

### Redis Security

Secure your Redis instance:
```conf
bind 127.0.0.1 your-private-ip
requirepass your-strong-password
protected-mode yes
```

### Network Security

- Use private networks between servers
- Configure firewall rules
- Use Redis AUTH if on public networks
- Regularly update all components

## Support

For issues with network functionality:
1. Check this guide first
2. Enable debug mode and check logs
3. Test Redis connectivity manually
4. Verify Velocity plugin is loaded
5. Check server configurations match

The network system is designed to be fault-tolerant - if Redis goes down, servers continue working in standalone mode and reconnect automatically when Redis is restored.