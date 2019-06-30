# JUBEI'THOS blademaster : indexer

![jubei](../_/img/jubei.jpg)

# [LORE](https://wow.gamepedia.com/Jubei%27Thos)

Jubei'Thos and the Blackrock warlocks sensed that the second coming of
the Burning Legion was at hand, and took steps to prepare. Jubei'Thos
commanded the Slave Master to capture many humans in the town of
Strahnbrad and to the Blademaster of the Blackrock Clan to sacrifice
them to appease their demon masters. The Slave Master was killed but
had succeded in capturing villagers the Blademaster would later
sacrifice. Arthas and Uther discovered this and raided the Blackrock
clan village, killing the Blademaster. Uther and Arthas headed home,
and Jubei'Thos led the orcs to the Alterac Mountains.

The Blackrock clan built a Demon Gate to communicate with their demon
masters. When Kel'Thuzad was reborn as a lich, he found out that the
Demon Gate was needed to commune with the demon lord Archimonde. In an
effort to reclaim it Arthas and Kel'thuzad battled the orcs guarding
it. Jubei'Thos and his orcs believed that the Scourge was an impure
race to the demons, and that his people were truly the chosen of the
Burning Legion. Jubei'Thos and his clanmates died defending the gate
against Kel'thuzad and Arthas.


# building indexes by slashing

consume events from the topic, for each tag K:V build append the
offset(uint64) to `root/topic/forward.bin`, then gets this offset and
builds inverted index in `root/topic/tagKey/tagValue.p` which is
searched by [khanzo](../khanzo)


