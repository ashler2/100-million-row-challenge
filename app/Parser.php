<?php

declare(strict_types=1);

namespace App;

use App\Commands\Visit;
use function array_fill;
use function fclose;
use function fgets;
use function file_get_contents;
use function file_put_contents;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function fwrite;
use function getmypid;
use function is_dir;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function sys_get_temp_dir;
use function unlink;
use function unpack;
use const SEEK_CUR;

final class Parser
{
    private const int WORKERS = 8;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
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

        if ($lastNl !== false) {
            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 52);

                $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
                if (!isset($pathIds[$path])) {
                    $pathIds[$path] = $pathCount;
                    $paths[$pathCount] = $path;
                    $pathCount++;
                }

                $pos = $nlPos + 1;
            }
        }
        unset($chunk);

        foreach (Visit::all() as $visit) {
            $path = substr($visit->uri, 25);
            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $paths[$pathCount] = $path;
                $pathCount++;
            }
        }

        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid = getmypid();
        $tmpFiles = [];
        $pids = [];

        for ($i = 0; $i < $workers - 1; $i++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $i;
            $tmpFiles[$i] = $tmpFile;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = self::processChunk(
                    $inputPath, $boundaries[$i], $boundaries[$i + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('V*', ...$data));
                exit(0);
            }

            $pids[$i] = $pid;
        }

        $mergedCounts = self::processChunk(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

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
            $base = $pathId * $dateCount;
            $buf = '';
            $sep = "\n";

            for ($dateId = 0; $dateId < $dateCount; $dateId++) {
                $count = $mergedCounts[$base + $dateId];
                if ($count === 0) {
                    continue;
                }
                $buf .= "{$sep}        \"20{$dates[$dateId]}\": {$count}";
                $sep = ",\n";
            }

            if ($buf === '') {
                continue;
            }

            fwrite($out, ($firstPath ? '' : ',') . "\n    \"\/blog\/{$path}\": {" . $buf . "\n    }");
            $firstPath = false;
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
    ): array {
        $stride = $dateCount;
        $counts = array_fill(0, $pathCount * $stride, 0);

        if ($start >= $end) {
            return $counts;
        }

        $pathBases = [];
        foreach ($pathIds as $p => $id) {
            $pathBases[$p] = $id * $stride;
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);

        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > 1_048_576 ? 1_048_576 : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) {
                break;
            }
            if ($lastNl < $chunkLen - 1) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 52);

                $counts[$pathBases[substr($chunk, $pos + 25, $nlPos - $pos - 51)] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }
}
