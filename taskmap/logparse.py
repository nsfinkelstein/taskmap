def in_progress(path_to_log):
    with open(path_to_log, 'r') as f:
        log = f.readlines()

    queued = []
    started = []
    finished = []
    for line in log:
        words = line.strip().split(' ')

        if 'starting' in words:
            started.append(words[-1])
        elif 'finished' in words:
            finished.append(words[-1])
        elif 'queueing' in words:
            queued.append(words[-1])

    return {
        'in_progress': set(started) - set(finished),
        'queued': set(queued) - set(started),
    }
