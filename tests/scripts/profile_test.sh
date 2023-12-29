test_file="tests/test_retrieve.py"
test_name="test_retrieve"
cd ../../
output_file="${TEMPDIR}/${test_name}_profile"
python -m cProfile -o ../../ -m pytest ${test_file}:${test_name} -s campaigns
python -c "import pstats; pstats.Stats('${output_file}').strip_dirs().sort_stats('cumtime').print_stats(50)"
