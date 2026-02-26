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
use function str_replace;
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
    private const int CHUNK_BYTES = 8_388_608;
    private const int PROBE_BYTES = 1_048_576;

    public function parse(string $inputPath, string $outputPath): void
    {
        gc_disable();
        $fileSize = filesize($inputPath);
        $workers = self::WORKERS;

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
                $prefix = $yStr . '-' . $mStr . '-';
                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $key = $prefix . (($d < 10 ? '0' : '') . $d);
                    $dateIds[$key] = $dateCount;
                    $dates[$dateCount] = $key;
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $warmUpSize = $fileSize > self::PROBE_BYTES ? self::PROBE_BYTES : $fileSize;
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
                if ($nlPos === false) break;

                $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
                if (!isset($pathIds[$path])) {
                    $pathIds[$path] = $pathCount * $dateCount;
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
                $pathIds[$path] = $pathCount * $dateCount;
                $paths[$pathCount] = $path;
                $pathCount++;
            }
        }

        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < $workers; $i++) {
            fseek($handle, (int) ($fileSize / $workers * $i));
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        fclose($handle);
        $boundaries[] = $fileSize;

        $tmpDir = is_dir('/dev/shm') ? '/dev/shm' : sys_get_temp_dir();
        $myPid = getmypid();
        $children = [];

        for ($i = 0; $i < $workers - 1; $i++) {
            $tmpFile = $tmpDir . '/p100m_' . $myPid . '_' . $i;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = self::processChunk(
                    $inputPath, $boundaries[$i], $boundaries[$i + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('V*', ...$data));
                exit(0);
            }

            $children[] = [$pid, $tmpFile];
        }

        $mergedCounts = self::processChunk(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($children as [$cpid, $tmpFile]) {
            pcntl_waitpid($cpid, $status);
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

        for ($p = 0; $p < $pathCount; $p++) {
            $base = $p * $dateCount;
            $buf = '';
            $sep = '';

            for ($d = 0; $d < $dateCount; $d++) {
                $count = $mergedCounts[$base + $d];
                if ($count === 0) continue;
                $buf .= $sep . '        "20' . $dates[$d] . '": ' . $count;
                $sep = ",\n";
            }

            if ($buf === '') continue;

            fwrite($out, ($firstPath ? '' : ',') . "\n    \"\\/blog\\/" . str_replace('/', '\\/', $paths[$p]) . "\": {\n" . $buf . "\n    }");
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
        $counts = array_fill(0, $pathCount * $dateCount, 0);
        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > self::CHUNK_BYTES ? self::CHUNK_BYTES : $remaining);
            $chunkLen = strlen($chunk);
            if ($chunkLen === 0) break;
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");
            if ($lastNl === false) break;

            $tail = $chunkLen - $lastNl - 1;
            if ($tail > 0) {
                fseek($handle, -$tail, SEEK_CUR);
                $remaining += $tail;
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 52);

                $counts[$pathIds[substr($chunk, $pos + 25, $nlPos - $pos - 51)] + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }
}
