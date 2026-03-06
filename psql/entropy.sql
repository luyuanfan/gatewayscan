-- i lowkey don't think shannon entropy is a good idea because 
-- it does not account for the location at which things appear,
-- but only the probability

CREATE FUNCTION shannon_bin(hid text)
  RETURNS float
AS $$
  from collections import Counter
  from math import log2
  binary = bin(int(hid, 16))[2:].zfill(64)
  c = Counter(binary)
  score = - sum([(val / 64) * log2(val / 64) for key, val in c.items()])
  return score
$$ LANGUAGE plpython3u;