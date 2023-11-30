import pypoolparty


def test_jobname_ichunk():
    ichunks = [
        1000 * 1000 * 1000 * i + 1000 * 1000 * i + 1000 * i + i
        for i in range(1000)
    ]
    for ichunk in ichunks:
        jobname = pypoolparty.pooling.make_jobname_from_ichunk(
            session_id="hans",
            ichunk=ichunk,
        )
        ichunk_back = pypoolparty.pooling.make_ichunk_from_jobname(
            jobname=jobname
        )
        assert ichunk_back == ichunk
