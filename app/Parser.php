<?php

declare(strict_types=1);

namespace App;

use const SEEK_CUR;
use const STREAM_IPPROTO_IP;
use const STREAM_PF_UNIX;
use const STREAM_SOCK_STREAM;

final class Parser
{
    private const int WORKERS = 8;

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

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > 16_777_216 ? 16_777_216 : $fileSize;
        $chunk = fread($handle, $warmUpSize);
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");

        $pathIds = [];
        $paths = [];
        $pathCount = 0;

        $dateIds = [];
        $dates = [];
        $dateCount = 0;

        $warmUpCounts = [];

        if ($lastNl !== false) {
            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 55);

                $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
                $pathId = $pathIds[$path] ?? $pathCount;

                if ($pathId === $pathCount) {
                    $pathIds[$path] = $pathId;
                    $paths[$pathCount] = $path;
                    $pathCount++;
                }

                $dateStr = substr($chunk, $nlPos - 23, 8);
                $dateId = $dateIds[$dateStr] ?? -1;

                if ($dateId === -1) {
                    $dateId = $dateCount;
                    $dateIds[$dateStr] = $dateId;
                    $dates[$dateCount] = $dateStr;
                    $dateCount++;
                }

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

        $pipes = [];
        $pids = [];

        for ($i = 0; $i < $workers - 1; $i++) {
            $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
            $pid = pcntl_fork();

            if ($pid === 0) {
                foreach ($pipes as $p) {
                    fclose($p);
                }
                fclose($pair[0]);
                $data = self::processChunk(
                    $inputPath, $boundaries[$i], $boundaries[$i + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount, $quickPath,
                );
                $binary = pack('V*', ...$data);
                $len = strlen($binary);
                $written = 0;
                while ($written < $len) {
                    $w = fwrite($pair[1], substr($binary, $written, 262144));
                    $written += $w;
                }
                fclose($pair[1]);
                exit(0);
            }

            fclose($pair[1]);
            $pipes[$i] = $pair[0];
            $pids[$i] = $pid;
        }

        $parentCounts = self::processChunk(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount, $quickPath,
        );

        $total = $pathCount * $dateCount;
        $mergedCounts = $warmUpFlat;
        unset($warmUpFlat);

        foreach ($pipes as $i => $pipe) {
            $wCounts = unpack('V*', stream_get_contents($pipe));
            fclose($pipe);
            pcntl_waitpid($pids[$i], $status);

            $j = 0;
            foreach ($wCounts as $v) {
                $mergedCounts[$j++] += $v;
            }
        }

        for ($j = 0; $j < $total; $j++) {
            $mergedCounts[$j] += $parentCounts[$j];
        }
        unset($parentCounts);

        $sortedDates = $dates;
        asort($sortedDates);

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

            foreach ($sortedDates as $dateId => $dateStr) {
                $count = $mergedCounts[$base + $dateId];
                if ($count === 0) {
                    continue;
                }
                $buf .= "{$sep}        \"20{$dateStr}\": {$count}";
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
            $chunk = fread($handle, $remaining > 4_194_304 ? 4_194_304 : $remaining);
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
                    $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
                    $pathId = $pathIds[$path] ?? $pathCount;
                    if ($pathId === $pathCount) {
                        $pathIds[$path] = $pathId;
                        for ($j = 0; $j < $stride; $j++) {
                            $counts[($pathCount * $stride) + $j] = 0;
                        }
                        $pathCount++;
                    }
                }

                $dateId = $dateIds[substr($chunk, $nlPos - 23, 8)] ?? -1;
                if ($dateId === -1) {
                    $dateStr = substr($chunk, $nlPos - 23, 8);
                    $dateId = $dateCount;
                    $dateIds[$dateStr] = $dateId;
                    $newStride = $stride + 1;
                    $newCounts = array_fill(0, $pathCount * $newStride, 0);
                    for ($j = 0; $j < $pathCount; $j++) {
                        $srcBase = $j * $stride;
                        $dstBase = $j * $newStride;
                        for ($k = 0; $k < $dateCount; $k++) {
                            $newCounts[$dstBase + $k] = $counts[$srcBase + $k];
                        }
                    }
                    $counts = $newCounts;
                    $stride = $newStride;
                    $dateCount++;
                }

                $counts[($pathId * $stride) + $dateId]++;
                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }
}
