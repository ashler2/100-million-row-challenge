<?php

declare(strict_types=1);

namespace App;

use function array_fill;
use function chr;
use function fclose;
use function fgets;
use function filesize;
use function fopen;
use function fread;
use function fseek;
use function ftell;
use function ftok;
use function fwrite;
use function pack;
use function pcntl_fork;
use function pcntl_waitpid;
use function shmop_delete;
use function shmop_open;
use function shmop_read;
use function shmop_write;
use function stream_set_read_buffer;
use function stream_set_write_buffer;
use function strlen;
use function strpos;
use function strrpos;
use function substr;
use function unpack;
use const SEEK_CUR;

final class Parser
{
    private const int WORKERS = 3;

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
                $nlPos = strpos($chunk, "\n", $pos + 55);

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

        $total = $pathCount * $dateCount;
        $shmSize = $total * 4;
        $shmIds = [];
        $pids = [];

        for ($i = 0; $i < $workers - 1; $i++) {
            $shmKey = ftok($inputPath, chr(65 + $i));
            $shmId = shmop_open($shmKey, 'c', 0644, $shmSize);
            $shmIds[$i] = [$shmKey, $shmId];
            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = self::processChunk(
                    $inputPath, $boundaries[$i], $boundaries[$i + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                shmop_write($shmId, pack('V*', ...$data), 0);
                exit(0);
            }

            $pids[$i] = $pid;
        }

        $parentCounts = self::processChunk(
            $inputPath, $boundaries[$workers - 1], $boundaries[$workers],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        $mergedCounts = $parentCounts;
        unset($parentCounts);

        foreach ($shmIds as [$shmKey, $shmId]) {
            $wCounts = unpack('V*', shmop_read($shmId, 0, $shmSize));
            shmop_delete($shmId);
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
            $chunk = fread($handle, $remaining > 33_554_432 ? 33_554_432 : $remaining);
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

                $counts[$pathIds[substr($chunk, $pos + 25, $nlPos - $pos - 51)] * $stride + $dateIds[substr($chunk, $nlPos - 23, 8)]]++;

                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }
}
