<?php

namespace App;

final class Parser
{
    public function parse(string $inputPath, string $outputPath): void
    {
        $fileSize = filesize($inputPath);

		$fh = fopen($inputPath, 'r');
		fseek($fh, (int) ($fileSize / 2));
		fgets($fh);
		$splitPoint = ftell($fh);
		fclose($fh);

        $tmpFile = tempnam(sys_get_temp_dir(), 'parse_');
        $childPid = pcntl_fork();

        if ($childPid === -1) {
            $data = $this->processChunk($inputPath, 0, $fileSize);
        } elseif ($childPid === 0) {
            $data = $this->processChunk($inputPath, $splitPoint, $fileSize);
            file_put_contents($tmpFile, function_exists('igbinary_serialize') ? igbinary_serialize($data) : serialize($data));
            exit(0);
        } else {
            $data = $this->processChunk($inputPath, 0, $splitPoint);
            pcntl_waitpid($childPid, $status);

            $raw = file_get_contents($tmpFile);
            $childData = function_exists('igbinary_unserialize') ? igbinary_unserialize($raw) : unserialize($raw);
            unlink($tmpFile);

            foreach ($childData as $path => $dates) {
                if (isset($data[$path])) {
                    foreach ($dates as $date => $count) {
                        $data[$path][$date] = ($data[$path][$date] ?? 0) + $count;
                    }
                } else {
                    $data[$path] = $dates;
                }
            }
        }

        foreach ($data as $path => &$dates) {
            ksort($dates);
        }
        unset($dates);

        file_put_contents($outputPath, json_encode($data, JSON_PRETTY_PRINT));
    }

    private function processChunk(string $filePath, int $start, int $end): array
    {
        $data = [];
        $bufSize = 8 * 1024 * 1024;
        $fh = fopen($filePath, 'r');
        stream_set_read_buffer($fh, 0);
        fseek($fh, $start);

        $remaining = $end - $start;
        $remainder = '';

        while ($remaining > 0) {
            $readLen = min($bufSize, $remaining);
            $chunk = fread($fh, $readLen);
            $remaining -= $readLen;

            if ($remainder !== '') {
                $chunk = $remainder . $chunk;
                $remainder = '';
            }

            $chunkLen = strlen($chunk);
            $lastNl = strrpos($chunk, "\n");

            if ($lastNl === false) {
                $remainder = $chunk;
                continue;
            }

            if ($lastNl < $chunkLen - 1) {
                $remainder = substr($chunk, $lastNl + 1);
            }

            $pos = 0;
            while ($pos <= $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos);
                if ($nlPos === false || $nlPos > $lastNl) {
                    break;
                }

                $commaPos = $nlPos - 26;
                $path = substr($chunk, $pos + 19, $commaPos - $pos - 19);
                $date = substr($chunk, $commaPos + 1, 10);

                if (isset($data[$path][$date])) {
                    $data[$path][$date]++;
                } else {
                    $data[$path][$date] = 1;
                }

                $pos = $nlPos + 1;
            }
        }

        fclose($fh);

        if ($remainder !== '') {
            $len = strlen($remainder);
            $commaPos = $len - 25;
            $path = substr($remainder, 19, $commaPos - 19);
            $date = substr($remainder, $commaPos + 1, 10);

            if (isset($data[$path][$date])) {
                $data[$path][$date]++;
            } else {
                $data[$path][$date] = 1;
            }
        }

        return $data;
    }
}
