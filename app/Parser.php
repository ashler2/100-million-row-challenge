<?php

declare(strict_types=1);

namespace App;

use const SEEK_CUR;

final class Parser
{
    private const int WORKERS = 4;

    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);
        $workers = self::WORKERS;

        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workers; $i++) {
            fseek($handle, (int) ($fileSize / $workers * $i));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        fclose($handle);
        $boundaries[] = $fileSize;

        $dateIds = [];
        $dates = [];
        $dateCount = 0;
        for ($y = 20; $y <= 26; $y++) {
            $yStr = ($y < 10 ? '0' : '') . $y;
            for ($m = 1; $m <= 12; $m++) {
                $mStr = ($m < 10 ? '0' : '') . $m;
                $daysInMonth = match ($m) {
                    2 => (($y + 2000) % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $dStr = ($d < 10 ? '0' : '') . $d;
                    $key = $yStr . '-' . $mStr . '-' . $dStr;
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > 16_777_216 ? 16_777_216 : $fileSize;
        $chunk = fread($handle, $warmUpSize);
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");

        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $warmUpCounts = [];

        if ($lastNl !== false) {
            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 55);

                $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
                $pathId = $pathIds[$path] ?? -1;

                if ($pathId === -1) {
                    $pathId = $pathCount;
                    $pathIds[$path] = $pathId;
                    $paths[$pathCount] = $path;
                    $pathCount++;
                }

                $dateId = $dateIds[substr($chunk, $nlPos - 23, 8)];
                $warmUpCounts[$pathId][$dateId] = ($warmUpCounts[$pathId][$dateId] ?? 0) + 1;
                $pos = $nlPos + 1;
            }
        }
        unset($chunk);

        $warmUpEnd = $lastNl !== false ? $lastNl + 1 : 0;
        for ($i = 0; $i < $workers; $i++) {
            if ($boundaries[$i] < $warmUpEnd) {
                $boundaries[$i] = $warmUpEnd;
            }
        }

        $warmUpFlat = array_fill(0, $pathCount * $dateCount, 0);
        foreach ($warmUpCounts as $pId => $dateCounts) {
            $base = $pId * $dateCount;
            foreach ($dateCounts as $dId => $count) {
                $warmUpFlat[$base + $dId] = $count;
            }
        }
        unset($warmUpCounts);

        $quickPath = [];
        foreach ($paths as $id => $p) {
            $pLen = strlen($p);
            $fc = $p[0];
            $lc = $p[$pLen - 1];
            if (!isset($quickPath[$pLen][$fc][$lc])) {
                $quickPath[$pLen][$fc][$lc] = $id;
            } else {
                $quickPath[$pLen][$fc][$lc] = -1;
            }
        }

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $tmpFiles = [];
        $pids = [];

        for ($i = 0; $i < $workers - 1; $i++) {
            $tmpFile = $tmpDir . '/parse_' . $myPid . '_' . $i;
            $tmpFiles[$i] = $tmpFile;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = self::processChunk(
                    $inputPath, $boundaries[$i], $boundaries[$i + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount, $quickPath,
                );
                file_put_contents($tmpFile, pack('V*', ...$data));
                exit(0);
            }

            $pids[$i] = $pid;
        }

        $parentCounts = self::processChunk(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount, $quickPath,
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $total = $pathCount * $dateCount;
        $mergedCounts = $warmUpFlat;
        unset($warmUpFlat);

        for ($j = 0; $j < $total; $j++) {
            $mergedCounts[$j] += $parentCounts[$j];
        }
        unset($parentCounts);

        foreach ($tmpFiles as $tmpFile) {
            $wCounts = unpack('V*', file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $mergedCounts[$j++] += $v;
            }
        }

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);

        fwrite($out, '{');
        $firstPath = true;

        foreach ($paths as $pathId => $path) {
            $buf = $firstPath ? '' : ',';
            $firstPath = false;
            $buf .= "\n    \"\/blog\/{$path}\": {";

            $base = $pathId * $dateCount;
            $sep = "\n";

            for ($dateId = 0; $dateId < $dateCount; $dateId++) {
                $count = $mergedCounts[$base + $dateId];
                if ($count === 0) {
                    continue;
                }
                $buf .= "{$sep}        \"20{$dates[$dateId]}\": {$count}";
                $sep = ",\n";
            }

            $buf .= "\n    }";
            fwrite($out, $buf);
        }

        fwrite($out, "\n}");
        fclose($out);
    }

    private static function processChunk(
        string $inputPath,
        int $start,
        int $end,
        array $pathIds,
        array $dateIds,
        int $pathCount,
        int $dateCount,
        array $quickPath,
    ): array {
        $stride = $dateCount;
        $counts = array_fill(0, $pathCount * $stride, 0);

        if ($start >= $end) {
            return $counts;
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > 8_388_608 ? 8_388_608 : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }
            if ($lastNl < ($chunkLen - 1)) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 55);

                $pathId = $quickPath[$nlPos - $pos - 51][$chunk[$pos + 25]][$chunk[$nlPos - 27]] ?? -1;

                if ($pathId < 0) {
                    $pathId = $pathIds[substr($chunk, $pos + 25, $nlPos - $pos - 51)] ?? -1;
                }

                if ($pathId >= 0) {
                    $counts[($pathId * $stride) + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;
                }

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }
}
