#!/usr/bin/env bash
# ==============================================================================
# ByteForce: Cluster Node Pre-Flight Audit Script
# Run this script directly on VM2 (Apoorva) and VM3 (Lakshya)
# ==============================================================================

set -u

echo "=============================================================================="
echo " ByteForce Cluster Audit — $(date)"
echo " Host: $(hostname) | User: $(whoami)"
echo "=============================================================================="

echo -e "\n--- 1. SYSTEM IDENTITY & ARCHITECTURE ---"
echo "Architecture: $(uname -m)"
echo "Kernel: $(uname -r)"
if command -v lsb_release &>/dev/null; then
    lsb_release -d | cut -f2-
elif [ -f /etc/os-release ]; then
    grep PRETTY_NAME /etc/os-release | cut -d= -f2- | tr -d '"'
fi
echo "CPU Cores: $(nproc)"

echo -e "\n--- 2. MEMORY & DISK CAPACITY ---"
free -h | awk 'NR==1{print "Type\tTotal\tUsed\tFree\tAvailable"} NR==2{print "RAM\t"$2"\t"$3"\t"$4"\t"$7} NR==3{print "Swap\t"$2"\t"$3"\t"$4}'
echo ""
df -h / | awk 'NR==1{print "Filesystem\tSize\tUsed\tAvail\tUse%\tMounted on"} NR==2{print $1"\t"$2"\t"$3"\t"$4"\t"$5"\t"$6}'

echo -e "\n--- 3. ZEROTIER NETWORK STATUS ---"
if command -v zerotier-cli &>/dev/null; then
    echo "ZeroTier Status: $(sudo zerotier-cli status 2>/dev/null || zerotier-cli status 2>/dev/null || echo 'Need sudo for status')"
else
    echo "ZeroTier CLI not found in PATH"
fi
echo "Active 10.194.92.x IP:"
ip -4 addr show | grep -E "inet 10\.194\.92\." || echo "WARNING: No 10.194.92.x address found on this host!"

echo -e "\n--- 4. PEER PING REACHABILITY (ZeroTier Subnet) ---"
PEERS=(
    "VM1-Medha:10.194.92.188"
    "VM2-Apoorva:10.194.92.212"
    "VM3-Lakshya:10.194.92.196"
    "VM4-Naivadh:10.194.92.171"
)

for peer in "${PEERS[@]}"; do
    NAME="${peer%%:*}"
    IP="${peer##*:}"
    if ping -c 1 -W 2 "$IP" &>/dev/null; then
        echo "  [OK] Reachable -> $NAME ($IP)"
    else
        echo "  [FAIL] Unreachable -> $NAME ($IP)"
    fi
done

echo -e "\n--- 5. SSH KEYS & INTER-NODE REACHABILITY ---"
echo "SSH Directory:"
ls -la ~/.ssh/ 2>/dev/null | grep -E "id_.*|authorized_keys|known_hosts" || echo "  No SSH keys found in ~/.ssh/"

echo "Testing Outbound SSH (Port 22 non-interactive probe):"
SSH_TARGETS=(
    "pes2ug24cs274@10.194.92.188 (VM1)"
    "user@10.194.92.212 (VM2)"
    "pes2ug24cs250@10.194.92.196 (VM3)"
    "pes2ug24cs297@10.194.92.171 (VM4)"
)

for target in "${SSH_TARGETS[@]}"; do
    USER_HOST="${target%% *}"
    LABEL="${target##* }"
    ssh -o BatchMode=yes -o ConnectTimeout=3 -o StrictHostKeyChecking=no "$USER_HOST" "echo Connected" &>/dev/null
    STATUS=$?
    if [ $STATUS -eq 0 ]; then
        echo "  [OK Passwordless] SSH -> $USER_HOST $LABEL"
    elif [ $STATUS -eq 255 ]; then
        echo "  [Needs Key/Auth] SSH -> $USER_HOST $LABEL (Port open, requires key/password)"
    else
        echo "  [Error $STATUS] SSH -> $USER_HOST $LABEL"
    fi
done

echo -e "\n--- 6. SOFTWARE & RUNTIME AUDIT ---"
# Java
if command -v java &>/dev/null; then
    JAVA_VER=$(java -version 2>&1 | head -n 1)
    echo "  Java: $JAVA_VER (Path: $(which java))"
else
    echo "  Java: NOT INSTALLED"
fi

# Python
if command -v python3 &>/dev/null; then
    PY_VER=$(python3 --version)
    echo "  Python: $PY_VER (Path: $(which python3))"
else
    echo "  Python: NOT INSTALLED"
fi

# Ansible
if command -v ansible &>/dev/null; then
    ANS_VER=$(ansible --version | head -n 1)
    echo "  Ansible: $ANS_VER"
else
    echo "  Ansible: NOT INSTALLED"
fi

# Git
if command -v git &>/dev/null; then
    GIT_VER=$(git --version)
    echo "  Git: $GIT_VER"
else
    echo "  Git: NOT INSTALLED"
fi

# Hadoop
if command -v hadoop &>/dev/null; then
    echo "  Hadoop: $(hadoop version | head -n 1)"
else
    echo "  Hadoop: NOT in PATH (Checking common directories...)"
    ls -d /opt/hadoop* ~/hadoop* /usr/local/hadoop* 2>/dev/null || echo "    None found in /opt or ~"
fi

# Spark
if command -v spark-submit &>/dev/null; then
    echo "  Spark: $(spark-submit --version 2>&1 | grep version | head -n 1)"
else
    echo "  Spark: NOT in PATH (Checking common directories...)"
    ls -d /opt/spark* ~/spark* /usr/local/spark* 2>/dev/null || echo "    None found in /opt or ~"
fi

echo -e "\n--- 7. PORTS IN USE (ByteForce Cluster Ports) ---"
if command -v ss &>/dev/null; then
    ss -tulpn 2>/dev/null | grep -E ':(9000|9870|9864|9866|7077|8080|8081|8082|8000)\b' || echo "  All target cluster ports are currently free."
fi

echo "=============================================================================="
echo " Audit complete."
echo "=============================================================================="
