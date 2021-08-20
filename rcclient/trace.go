package rcclient

import (
	"math/rand"
)

var view_frequent = []float64{0.4743679868, 0.07781218316, 0.05864918279, 0.0401948648, 0.02854732684, 0.02655730097, 0.02029574916, 0.01561958844, 0.01099829345, 0.009409473255, 0.01198244754, 0.007435449893, 0.00716912248, 0.005570014962, 0.005187097866,
	0.00523281931, 0.004428121889, 0.02555257223, 0.004303530953, 0.003489689243, 0.003375385632, 0.003301088285, 0.002867877599, 0.00323479219, 0.002560400886, 0.00383145704, 0.001897439942, 0.002399232794, 0.001780850259, 0.001999170156,
	0.001549956965, 0.001602536626, 0.002247208992, 0.001512236773, 0.001423079957, 0.001447083715, 0.001410506559, 0.00122533471, 0.001133891821, 0.001335066176, 0.0009830110543, 0.003648571262, 0.0009430047905, 0.0009864401626, 0.001044735004,
	0.001010443921, 0.001105315918, 0.0009704376571, 0.000885852985, 0.0008081265295, 0.0007246848935, 0.000708682388, 0.0007555468685, 0.0007875518796, 0.0007212557852, 0.0006309559325, 0.0006263837881, 0.001883723509, 0.0005726610909,
	0.0005978078853, 0.0005920927048, 0.0006663900519, 0.0006686761241, 0.0006080952103, 0.0006629609436, 0.0005772332354, 0.0005406560799, 0.007181695877, 0.0007326861463, 0.0005475142965, 0.0005166523216, 0.0005143662493, 0.0004995067799,
	0.0005075080327, 0.0006503875464, 0.0005086510688, 0.0004503562272, 0.0004400689022, 0.0004743599855, 0.0005143662493, 0.0004640726605, 0.0005555155493, 0.0004240663967, 0.0006743913047, 0.0004046347828, 0.0004023487106, 0.0004514992633,
	0.0003897753134, 0.0004514992633, 0.000698395063, 0.0004194942522, 0.0004434980105, 0.0004149221078, 0.0004229233606, 0.001959163892, 0.001984310686, 0.002363798675, 0.001922586736, 0.002016315697, 0.002083754828, 0.001967165145,
	0.0007692633018, 0.003136491085, 0.0006926798825, 0.0009132858516, 0.0006515305825, 0.0006583887992, 0.0005898066326, 0.0005966648492, 0.0005669459104, 0.0005303687549, 0.0005223675021, 0.0006343850409, 0.0006995380991, 0.001049307149,
	0.0006561027269, 0.0005395130438, 0.0005280826827, 0.0005177953577, 0.0005223675021, 0.0004229233606, 0.0004675017688, 0.0003840601328, 0.0003794879884, 0.0003806310245, 0.0004366397939, 0.0004537853355, 0.0003714867356, 0.001258482757,
	0.000250324908, 0.0003257652913, 0.0002628983052, 0.000344053869, 0.0002468957997, 0.0003017615329, 0.0002834729552, 0.0002971893885, 0.0003006184968, 0.0003143349302, 0.0003109058218, 0.0002640413413, 0.0003223361829, 0.0003486260134,
	0.000406920855, 0.0002720425941, 0.0002743286663, 0.0003189070746, 0.000270899558, 0.0001737414887, 0.0002034604275, 0.0001863148859, 0.0001897439942, 0.0004286385411, 0.0001508807665, 0.0001703123803, 0.0002057464997, 0.0001931731025,
	0.0001863148859, 0.0002103186442, 0.0002263211497, 0.0002286072219, 0.0002480388358, 0.0003280513635, 0.0002743286663, 0.0006903938102, 0.0003029045691, 0.0003371956523, 0.0003760588801, 0.0003383386885, 0.0003120488579, 0.0003840601328,
	0.0002880450996, 0.0002903311719, 0.0003657715551, 0.0003703436995, 0.0003863462051, 0.0004194942522, 0.0004572144439, 0.0004983637438, 0.0005395130438, 0.0007201127491, 0.0009475769349, 0.0009464338988, 0.0006961089908, 0.0006412432575,
	0.0006126673548, 0.0005120801771, 0.0005818053798, 0.000667533088, 0.0007749784823, 0.0006926798825, 0.0005383700076, 0.0004537853355, 0.0005166523216, 0.0003737728079, 0.0003817740606, 0.0003726297717, 0.0003589133384, 0.0003406247607,
	0.0003566272662, 0.0002766147385, 0.0001577389831, 0.000197745247, 0.0002114616803, 0.0001417364776, 0.0005498003687, 0.0001737414887, 0.0001131605749, 0.0001383073693, 9.72E-05, 0.0001085884304, 0.0001794566692, 0.0002366084747,
	0.000146308622, 0.0001531668387, 0.0001165896832, 6.86E-05, 0.0001200187915, 0.0001017302138, 0.000197745247, 0.0001120175387, 8.57E-05, 9.72E-05, 9.94E-05, 0.000166883272, 8.57E-05, 9.94E-05, 0.0001988882831, 0.0003486260134, 0.0003109058218,
	0.0001268770082, 0.000104016286, 0.0001291630804, 0.0001383073693, 0.0002343224025, 0.0002434666914, 0.0001165896832, 0.0001817427414, 0.0001131605749, 8.80E-05, 0.0001085884304, 0.0001680263081, 0.0001063023582, 8.92E-05, 0.0001234478998,
	8.12E-05, 4.46E-05, 9.60E-05, 0.000114303611, 9.49E-05, 8.46E-05, 7.77E-05, 0.0001325921887, 8.69E-05, 6.52E-05, 0.0002286072219, 0.000104016286, 6.17E-05, 5.94E-05, 7.54E-05, 2.63E-05, 3.77E-05, 4.80E-05, 4.11E-05, 5.03E-05, 0.0001085884304,
	5.94E-05, 1.49E-05, 3.54E-05, 4.69E-05, 3.09E-05, 8.00E-06, 8.00E-06, 2.97E-05, 1.49E-05, 2.86E-05, 3.43E-06, 1.94E-05, 5.83E-05, 5.72E-06, 4.57E-06, 2.29E-06, 8.00E-06, 5.72E-06, 1.14E-06, 6.86E-06, 2.29E-06, 1.14E-06, 8.00E-06}
var size_distribution = []uint64{4096, 8192, 12288, 16384, 20480, 24576, 28672, 32768, 36864, 40960, 45056, 49152, 53248, 57344,
	61440, 65536, 69632, 73728, 77824, 81920, 86016, 90112, 94208, 98304, 102400, 106496, 110592, 114688, 118784, 122880, 126976,
	131072, 135168, 139264, 143360, 147456, 151552, 155648, 159744, 163840, 167936, 172032, 176128, 180224, 184320, 188416, 192512,
	196608, 200704, 204800, 208896, 212992, 217088, 221184, 225280, 229376, 233472, 237568, 241664, 245760, 249856, 253952, 258048,
	262144, 266240, 270336, 274432, 278528, 282624, 286720, 290816, 294912, 299008, 303104, 307200, 311296, 315392, 319488, 323584,
	327680, 331776, 335872, 339968, 344064, 348160, 352256, 356352, 360448, 364544, 368640, 372736, 376832, 380928, 385024, 401933,
	422030, 443131, 465288, 488552, 512980, 538629, 565560, 593838, 623530, 654707, 687442, 721814, 757905, 795800, 835590, 877370,
	921238, 967300, 1015665, 1066448, 1119771, 1175759, 1234547, 1296275, 1361089, 1429143, 1500600, 1575630, 1654412, 1737132, 1823989,
	1915188, 2010948, 2111495, 2217070, 2327924, 2444320, 2566536, 2694863, 2829606, 2971086, 3119641, 3275623, 3439404, 3611374, 3791943,
	3981540, 4180617, 4389648, 4609130, 4839587, 5081566, 5335644, 5602427, 5882548, 6176675, 6485509, 6809785, 7150274, 7507788, 7883177,
	8277336, 8691203, 9125763, 9582051, 10061154, 10564211, 11092422, 11647043, 12229395, 12840865, 13482908, 14157054, 14864906, 15608152,
	16388559, 17207987, 18068387, 18971806, 19920396, 20916416, 21962237, 23060349, 24213366, 25424035, 26695236, 28029998, 29431498,
	30903073, 32448227, 34070638, 35774170, 37562879, 39441023, 41413074, 43483728, 45657914, 47940810, 50337850, 52854743, 55497480,
	58272354, 61185972, 64245270, 67457534, 70830411, 74371931, 78090528, 81995054, 86094807, 90399547, 94919525, 99665501, 104648776,
	109881215, 115375275, 121144039, 127201241, 133561303, 140239368, 147251337, 154613904, 162344599, 170461829, 178984920, 187934167,
	197330875, 207197419, 217557290, 228435154, 239856912, 251849757, 264442245, 277664358, 291547576, 306124954, 321431202, 337502762,
	354377900, 372096795, 390701635, 410236717, 430748553, 452285980, 474900280, 498645294, 523577558, 549756436, 577244258, 606106471,
	636411795, 668232384, 701644004, 736726204, 773562514, 812240640, 852852672, 895495305, 940270071, 987283574, 1036647753, 1088480140,
	1142904148, 1200049355, 1260051823, 1323054414, 1389207135, 1458667491, 1531600866, 1608180909, 1688589955, 1773019453, 1861670425,
	1954753946, 2052491644, 2155116226, 2262872037, 2376015639, 2494816421, 2619557242, 2750535104, 2888061860, 3032464953, 3184088200,
	3343292610, 3510457241, 3685980103, 3870279108, 4063793064, 4266982717, 4480331853, 4704348445, 4939565868, 5186544161}
var ratio_distribution = []float64{0.3376547948, 0.06980997565, 0.06838416669, 0.04919268692, 0.04499497778, 0.03703231302, 0.0297893857, 0.02403376389, 0.01789549686, 0.01544930065, 0.01310104339, 0.01236764006, 0.009541076281, 0.008570797337, 0.008696068093, 0.007220150826,
	0.006386530888, 0.005842172514, 0.005434473145, 0.005407141344, 0.005047272628, 0.004587187307, 0.004635017959, 0.0044277518, 0.003867449875, 0.005739678259, 0.003118102991, 0.003026996987, 0.003008775786, 0.002792399026, 0.002705848322,
	0.002696737722, 0.004115713736, 0.002514525714, 0.002364200807, 0.00242797501, 0.002170600548, 0.001977000289, 0.001922336687, 0.001808454182, 0.001692294026, 0.001676350476, 0.001567023271, 0.001594355072, 0.001904115486, 0.001701404627,
	0.001740124679, 0.001653573975, 0.00153969147, 0.001384811263, 0.00132103706, 0.001198043954, 0.001298260559, 0.001248152256, 0.001138825051, 0.001100105, 0.0011092156, 0.001154768602, 0.001027220196, 0.001072773198, 0.001079606149,
	0.001049996697, 0.001084161449, 0.001040886097, 0.001127436801, 0.0009885001446, 0.0009406694925, 0.0009338365422, 0.0008882835401, 0.0009657236436, 0.0008905611902, 0.0008290646374, 0.0008495634884, 0.0009133376912, 0.0009019494407,
	0.000819954037, 0.0008108434366, 0.0007789563352, 0.0007630127845, 0.0007516245339, 0.0008450081882, 0.0008381752378, 0.0007584574842, 0.0007630127845, 0.0007652904346, 0.0007265703828, 0.0007994551861, 0.0006946832814, 0.000774401035,
	0.000731125683, 0.0007447915836, 0.0007493468838, 0.0006969609315, 0.0007174597824, 0.00287211678, 0.003295759699, 0.003377755103, 0.003291204399, 0.003272983198, 0.003336757401, 0.003058884088, 0.001355201811, 0.001159323902,
	0.001008998996, 0.001195766304, 0.001129714451, 0.001157046252, 0.001061384948, 0.001095549699, 0.001008998996, 0.0009680012937, 0.0009816671943, 0.001134269751, 0.001289149958, 0.002017997991, 0.001213987505, 0.001029497846,
	0.0009178929914, 0.0008358975877, 0.0008245093372, 0.0007812339853, 0.0008358975877, 0.0006832950309, 0.0007037938818, 0.0006559632296, 0.0006924056313, 0.0006536855795, 0.0006377420288, 0.002373311407, 0.0004532523705, 0.0004942500723,
	0.0004076993684, 0.0003803675672, 0.0004236429191, 0.000487417122, 0.0004988053725, 0.0005261371738, 0.0004965277224, 0.0005466360247, 0.0005466360247, 0.0004896947721, 0.0005375254243, 0.0005329701241, 0.0006924056313, 0.0004828618218,
	0.0004600853208, 0.0004623629709, 0.0004828618218, 0.0003188710144, 0.0003667016666, 0.0002938168633, 0.0003165933643, 0.0002778733125, 0.000266485062, 0.0002938168633, 0.0003667016666, 0.0003257039647, 0.0002938168633, 0.0003644240165,
	0.0003621463664, 0.0003621463664, 0.0004122546686, 0.000487417122, 0.0004464194202, 0.0005443583746, 0.0004919724222, 0.0005853560764, 0.0006081325775, 0.0005694125257, 0.0005215818735, 0.0005329701241, 0.00044186412, 0.000464640621,
	0.0005785231261, 0.0006126878777, 0.0006240761282, 0.0006719067803, 0.0006787397306, 0.000797177536, 0.0008723399894, 0.001152490952, 0.001384811263, 0.001375700662, 0.00110693795, 0.0009862224945, 0.0009611683434, 0.0007926222358,
	0.0009178929914, 0.001029497846, 0.001168434503, 0.00109782735, 0.000819954037, 0.0006969609315, 0.0005352477742, 0.0005785231261, 0.0005739678259, 0.0005466360247, 0.0005398030744, 0.0004805841717, 0.0004145323187, 0.0003712569668,
	0.0002323203105, 0.0002163767598, 0.0002140991097, 0.0002027108592, 0.0001913226086, 0.0001913226086, 0.0001639908074, 0.000154880207, 0.0001229931056, 0.000111604855, 0.0001753790579, 0.000154880207, 0.0001480472567, 0.0001526025569,
	0.0001389366563, 8.20E-05, 0.0001207154555, 9.11E-05, 7.52E-05, 0.0001002166045, 8.43E-05, 7.97E-05, 9.34E-05, 9.34E-05, 7.74E-05, 9.34E-05, 0.0001047719047, 0.0001161601552, 7.97E-05, 9.11E-05, 0.0001184378053, 0.0001002166045, 0.0001184378053,
	0.0001002166045, 9.11E-05, 0.0001252707557, 0.0001207154555, 8.88E-05, 9.34E-05, 0.0001298260559, 0.000132103706, 0.000111604855, 6.61E-05, 8.20E-05, 7.06E-05, 6.83E-05, 0.0001024942546, 0.0001070495548, 6.83E-05, 6.61E-05, 6.38E-05,
	0.000111604855, 8.66E-05, 7.52E-05, 9.57E-05, 7.06E-05, 4.10E-05, 3.19E-05, 4.33E-05, 2.96E-05, 2.28E-05, 3.87E-05, 3.19E-05, 4.10E-05, 3.42E-05, 4.10E-05, 2.28E-05, 3.87E-05, 3.87E-05, 1.82E-05, 1.37E-05, 1.14E-05, 2.05E-05, 1.37E-05, 2.05E-05,
	4.56E-06, 1.82E-05, 4.56E-06, 9.11E-06, 6.83E-06, 4.56E-06, 6.83E-06, 9.11E-06, 2.28E-06, 6.83E-06, 4.56E-06, 2.28E-06, 9.11E-06}

type TraceItem struct {
	size      uint64
	sizeRatio float64
	viewRatio float64
}

type Trace struct {
	Traces     []TraceItem
	NumObjects uint64
}

func (trace *Trace) SampleGet() chan uint64 {
	result := make(chan uint64)

	sizes := trace.SampleSizes()
	traceStartId := make([]uint64,len(trace.Traces))

	index := 0
	for objectId:=trace.NumObjects;objectId>=1;objectId -- {
		if objectId==trace.NumObjects || sizes[objectId] != sizes[objectId - 1] {
			traceStartId[index] = objectId
			index ++
		}
	}


	go func() {
		//Make sure results are identical.
		rand.Seed(0)
		for {
			found := -1
			for found < 0 {
				total_ratio := 1.0
				for j := range trace.Traces {
					r := rand.Float64() * total_ratio
					if r < trace.Traces[j].viewRatio {
						found = j
						break
					}
					total_ratio -= trace.Traces[j].viewRatio
				}
				//log.Println(found)
			}
			if found != len(trace.Traces) - 1 {
				if traceStartId[found] != traceStartId[found + 1] {
					result <- rand.Uint64()%(traceStartId[found]-traceStartId[found + 1]) + traceStartId[found + 1]
				}
			} else {
				if traceStartId[found] != 0 {
					result <- rand.Uint64() % traceStartId[found]
				}
			}
		}
	}()
	return result
}

//Objects are ranked by their sizes descending.
func (trace *Trace) SampleSizes() []uint64 {
	result := make([]uint64, trace.NumObjects)

	index := len(trace.Traces) - 1
	current := -trace.Traces[index].sizeRatio

	for _, trace := range trace.Traces {
		current += trace.sizeRatio
	}

	for objectId := uint64(0); objectId < trace.NumObjects; objectId++ {
		point := float64(trace.NumObjects-1-objectId) / float64(trace.NumObjects)
		for point < current && index > 0 {
			index--
			current -= trace.Traces[index].sizeRatio
		}
		result[objectId] = trace.Traces[index].size
	}

	return result

}

func GetTrace(minSize, maxSize, numObjects uint64) *Trace {
	sizePortion := 0.0
	viewPortion := 0.0
	for i := 0; i < len(size_distribution); i++ {
		if size_distribution[i] >= minSize && size_distribution[i] <= maxSize {
			sizePortion += ratio_distribution[i]
			viewPortion += view_frequent[i]
		}
	}
	res := make([]TraceItem, 0)
	for i := 0; i < len(size_distribution); i++ {
		if size_distribution[i] >= minSize && size_distribution[i] <= maxSize {
			res = append(res, TraceItem{size: size_distribution[i], sizeRatio: ratio_distribution[i] / sizePortion, viewRatio: view_frequent[i] / viewPortion})
		}
	}
	return &Trace{Traces: res, NumObjects: numObjects}
}