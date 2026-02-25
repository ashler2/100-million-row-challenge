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
            $flat = $this->processChunk($inputPath, 0, $fileSize);
        } elseif ($childPid === 0) {
            $flat = $this->processChunk($inputPath, $splitPoint, $fileSize);
            file_put_contents($tmpFile, igbinary_serialize($flat));
            exit(0);
        } else {
            $flat = $this->processChunk($inputPath, 0, $splitPoint);
            pcntl_waitpid($childPid, $status);

            $childFlat = igbinary_unserialize(file_get_contents($tmpFile));
            unlink($tmpFile);

            foreach ($childFlat as $key => $count) {
                $flat[$key] = ($flat[$key] ?? 0) + $count;
            }
        }

        $data = [];
        foreach ($flat as $key => $count) {
            $path = substr($key, 0, -11);
            $date = substr($key, -10);
            $data[$path][$date] = $count;
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

                $key = substr($chunk, $pos + 19, $nlPos - $pos - 34);

                if (isset($data[$key])) {
                    $data[$key]++;
                } else {
                    $data[$key] = 1;
                }

                $pos = $nlPos + 1;
            }
        }

        fclose($fh);

        if ($remainder !== '') {
            $key = substr($remainder, 19, strlen($remainder) - 34);

            if (isset($data[$key])) {
                $data[$key]++;
            } else {
                $data[$key] = 1;
            }
        }

        return $data;
    }
}
