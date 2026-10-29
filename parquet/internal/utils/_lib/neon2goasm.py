#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Converts the ELF object built from _lib/bit_packing_neon.c into Go assembly.
# Branches become Go branch instructions and constant-pool loads become VMOVQ
# pseudo-instructions, so the Go assembler sees every control-flow edge and no
# instruction touches the stack pointer or Go's reserved registers; the guard
# below rejects any input that does (GH-983).

import re, subprocess, sys

import shutil
OBJDUMP = shutil.which("llvm-objdump") or "/Library/Developer/CommandLineTools/usr/bin/llvm-objdump"
obj = sys.argv[1]

dis = subprocess.check_output([OBJDUMP, "-d", "-r", obj], text=True).splitlines()
sec = subprocess.check_output([OBJDUMP, "-s", "-j", ".rodata.cst16", obj], text=True).splitlines()

# ---- constant pool bytes ----
pool = {}
for line in sec:
    m = re.match(r"^ ([0-9a-f]+) ((?:[0-9a-f]{8} ){1,4})", line)
    if not m:
        continue
    addr = int(m.group(1), 16)
    chunk = m.group(2).split()
    data = b"".join(bytes.fromhex(w) for w in chunk)
    for i, b in enumerate(data):
        pool[addr + i] = b

def const128(off):
    lo = int.from_bytes(bytes(pool[off + i] for i in range(8)), "little")
    hi = int.from_bytes(bytes(pool[off + i] for i in range(8, 16)), "little")
    return lo, hi

# ---- instruction stream ----
insns = []  # (offset, enc, text)
relocs = {}  # instruction offset -> (type, target-offset)
for line in dis:
    m = re.match(r"^\s+([0-9a-f]+):\s+([0-9a-f]{8})\s+(.*)$", line)
    if m:
        insns.append((int(m.group(1), 16), int(m.group(2), 16), m.group(3).strip()))
        continue
    m = re.match(r"^\s+([0-9a-f]{16}):\s+(R_AARCH64_\w+)\s+\.rodata\.cst16(?:\+(0x[0-9a-f]+))?", line)
    if m:
        relocs[int(m.group(1), 16)] = (m.group(2), int(m.group(3) or "0", 16))

BRANCH = {
    "b.ne": "BNE", "b.eq": "BEQ", "b.lt": "BLT", "b.gt": "BGT",
    "b.le": "BLE", "b.ge": "BGE", "b.hs": "BHS", "b.lo": "BLO",
    "b.hi": "BHI", "b.ls": "BLS", "b.pl": "BPL", "b.mi": "BMI",
    "b": "JMP",
}

def parse_branch(text):
    parts = text.split()
    mnem = parts[0]
    if mnem in BRANCH:
        tgt = int(parts[1].rstrip(","), 16)
        return BRANCH[mnem], None, tgt
    if mnem in ("cbz", "cbnz"):
        reg = parts[1].rstrip(",")
        tgt = int(parts[2], 16)
        op = "CBZ" if mnem == "cbz" else "CBNZ"
        if reg.startswith("w"):
            op += "W"
        return op, "R" + reg[1:], tgt
    if mnem in ("tbz", "tbnz"):
        raise SystemExit(f"unhandled branch: {text}")
    return None

# ---- collect labels ----
targets = set()
for off, enc, text in insns:
    mnem = text.split()[0]
    if mnem in BRANCH or mnem in ("cbz", "cbnz"):
        b = parse_branch(text)
        if b:
            targets.add(b[2])

def label(off):
    return f"L{off:04x}"

# ---- emit ----
out = []
for off, enc, text in insns:
    if off in targets:
        out.append(f"\n{label(off)}:")
    mnem = text.split()[0]
    if mnem == "ret":
        out.append("\tMOVD R0, num+32(FP)")
        out.append("\tRET")
        continue
    if mnem in BRANCH or mnem in ("cbz", "cbnz"):
        op, reg, tgt = parse_branch(text)
        if reg:
            out.append(f"\t{op} {reg}, {label(tgt)}")
        else:
            out.append(f"\t{op} {label(tgt)}")
        continue
    if off in relocs:
        rtype, roff = relocs[off]
        if rtype == "R_AARCH64_ADR_PREL_PG_HI21":
            out.append(f"\t// {text} (constant page; folded into VMOVQ below)")
            continue
        if rtype == "R_AARCH64_LDST128_ABS_LO12_NC":
            m = re.match(r"ldr\s+q(\d+),", text)
            if not m:
                raise SystemExit(f"unexpected reloc use: {text}")
            lo, hi = const128(roff)
            out.append(f"\tVMOVQ ${lo:#018x}, ${hi:#018x}, V{m.group(1)} // {text}")
            continue
        raise SystemExit(f"unhandled reloc {rtype}: {text}")
    if re.search(r"\b(sp|x18|x27|x28|w18|w27|w28)\b", text):
        raise SystemExit(f"forbidden register/sp in: {off:#x} {text}")
    out.append(f"\tWORD $0x{enc:08x} // {text}")

HEADER = """\
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !noasm

// Code generated by _lib/neon2goasm.py. DO NOT EDIT.
//
// Generated from _lib/bit_packing_neon.c with the Go-reserved registers
// excluded (-ffixed-x18 -ffixed-x27 -ffixed-x28) and no frame or stack
// usage, so asynchronous profiling and preemption always observe valid
// unwind state (GH-983). Constant-pool loads use VMOVQ pseudo-instructions
// and every branch is visible to the Go assembler.

#include "textflag.h"

TEXT \u00b7_unpack32_neon(SB), NOSPLIT, $0-40
\tMOVD in+0(FP), R0
\tMOVD out+8(FP), R1
\tMOVD batchSize+16(FP), R2
\tMOVD nbits+24(FP), R3
"""

print(HEADER + "\n".join(out))
