from utils.api_utils import MyModules,api,build_logging
MyModules.load_all_modules()

if __name__ == "__main__":
    for name,module in MyModules.current_module_list.items():
        module.run_test()